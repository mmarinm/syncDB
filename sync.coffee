promiseBreak = require('promise-break')
elasticsearch = require('elasticsearch')
Ora = require('ora')
axios = require('axios')
bodybuilder = require('bodybuilder')

devDB = new elasticsearch.Client(
	host: "#{ENV.DEV_HOST}:#{ENV.DEV_PORT}"
	# log: 'trace'
)

prodDB = new elasticsearch.Client(
	host: "#{ENV.PROD_HOST}:#{ENV.PROD_PORT}"
	# log: 'trace'
)

optionsDefault={
	'campaign': ['campaign/lead', 'campaign/serverclick', 'campaign/clientclick']
	'click' : ['campaign/serverclick', 'campaign/clientclick']
	'lead' : ['campaign/lead']
}

spinner = new Ora()

syncDB = (options)->
	toSyncArr = devDbTotalCnt = prodDbTotalCnt = null

	Promise.resolve(options)
		.tap () -> spinner.start('start syncing devDB with prodDB')
		.then ()-> checkArgs options
		.then (toSync)-> toSyncArr=toSync  
		.then ()-> checkConnection prodDB, 'prodDB'
		.then ()-> checkConnection devDB, 'devDB'
		.then ()-> toSyncArr
		.then (toSyncArr)-> countRecordsForDB(toSyncArr, devDB, 'devDB')
		.then (totalCnt)-> devDbTotalCnt=totalCnt
		.then ()-> toSyncArr
		.then (toSyncArr)-> countRecordsForDB(toSyncArr, prodDB, 'prodDB')
		.then (totalCnt)-> prodDbTotalCnt=totalCnt
		.then () -> cntDiff(prodDbTotalCnt,devDbTotalCnt)

		.then ()-> toSyncArr
		.map (indtype)-> syncType indtype
		.catch promiseBreak.end

		#if DBs in syc break
		.then ()->
			spinner.succeed('Everything is in sync now')
		.catch (err)->
			spinner.fail()
			console.error(err)

checkArgs = (options=optionsDefault)->

	Promise.resolve(options)
		.then (options)-> 
			typesToSync = []
			if options.types
				for option in options.types
					typesToSync.push(option)
				return typesToSync
			if options.template
				for option in optionsDefault[options.template]
					typesToSync.push(option)
				return typesToSync
			if options is optionsDefault
				Object.keys(optionsDefault).forEach (index)->
					optionsDefault[index].forEach (command)->
						typesToSync.push command
				return typesToSync.unique()
			else 
				promiseBreak('illegal argument')


checkConnection = (db, tag)->
	Promise.resolve()
		.then ()-> db.ping requestTimeout:1000			
		.then (resp)-> spinner.succeed("#{tag} connected")


countRecordsForDB = (typesToSync, db, nameofdb)->
	Promise.resolve(typesToSync)
		.map (indtype)->  countType(indtype, db)
		.then (cntarr)-> totalCnt = cntarr.sum()
		.tap (totalCnt)-> spinner.info("You have #{totalCnt} records for selected types in #{nameofdb}")


cntDiff = (prodDbTotalCnt, devDbTotalCnt)->
	Promise.resolve()
		.then () -> 
			if prodDbTotalCnt - devDbTotalCnt is 0
				promiseBreak('Selected types are in sync')
			else
				prodDbTotalCnt - devDbTotalCnt
		.tap (diff)-> spinner.info("Prod DB is #{diff} records ahead Dev DB for selected types")


countType = (indtype, db)-> 
	it = indtype.split('/')
	index = it[0]
	type = it[1]
	Promise.resolve()
		.then ()->  db.count({index, type})
		.then (cnt)-> cnt.count

syncType = (indtype)->
	typeProps = {}
	typeProps.indtype = indtype
	typeProps.chunkSize = 5000
	typeProps.moved = 0
	typeProps.lastdate = 0
	
	Promise.resolve()
		#get last date from db
		.then ()-> getLastRecord(devDB, indtype)
		.then (lastRec)-> if lastRec then typeProps.lastdate = lastRec['_source'].date
		.then ()-> countType(indtype, prodDB)
		.then (cnt)-> typeProps.totalToMove = cnt
		.then ()-> moveChunk(typeProps)


getLastRecord = (db, indtype)-> 
	index = indtype.split('/')[0]
	type = indtype.split('/')[1]
	
	body = bodybuilder()
		.size(1)
		.sort([date:'desc'])


	Promise.resolve()
		.then ()-> db.search {index, type, body}
		.then (res) -> res.hits.hits[0]

moveChunk = (props)-> 
	# console.log props.moved, props.totalToMove
	return if props.moved >= props.totalToMove

	Promise.resolve()
		.then ()-> getChunk(props)
		.then (res)-> prepForInsertion(res.hits.hits)
		.then writeChunk
		.then ()-> props.moved += props.chunkSize
		.then ()-> moveChunk(props)



getChunk = (props)->
	# console.log props.lastdate, 'last date'
	it = props.indtype.split('/')
	index = it[0]
	type = it[1]
	
	# body = bodybuilder()
	# 	.filter('range', 'date','gte': props.lastdate)
	# 	.build()

	Promise.resolve()
		.then ()-> prodDB.search {
			index, type,
			size: props.chunkSize
			# from: 0
			body:
				sort: [{date:'desc'}]
				search_after: [(new Date(props.lastdate)).valueOf()]
		}
		.tap (res)-> 
			console.log res.hits.hits.length
			if res.hits.hits[res.hits.hits.length-1] is undefined
				console.dir(res, colors:1, depth:999)
				console.log new Date(props.lastdate)
				process.exit()
			props.lastdate = res.hits.hits[res.hits.hits.length-1]._source.date



writeChunk = (data)->
	Promise.resolve(data)
		.then (body)-> devDB.bulk {body}

prepForInsertion = (data)-> 
	body = []

	for doc in data
		body.push JSON.stringify index:{_index:doc._index, _type:doc._type, _id:doc._id}
		body.push JSON.stringify doc._source

	
	return body.join '\n'

	

module.exports=syncDB