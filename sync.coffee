promiseBreak = require('promise-break')
elasticsearch = require('elasticsearch')
ora = require('ora')()
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


syncDB = (options)->
	toSyncArr = prodDataMap = devDbTotalCnt = prodDbTotalCnt = null

	Promise.resolve(options)
		# .tap () -> ora.log('syncing elastic dbs')
		.then ()-> checkArgs options
		.then (toSync)-> toSyncArr=toSync  
		.then ()-> checkConnection prodDB
		.then ()-> checkConnection devDB
		# .then ()-> getIndeces prodDB
		# .map (indx)-> getTypes indx
		# .then (mappings)-> prodDataMap = mappings[0]
		# .then ()-> countDiff prodDataMap
		.then ()-> toSyncArr
		.then (toSyncArr)-> countRecordsForDB(toSyncArr, devDB, 'devDB')
		.then (totalCnt)-> devDbTotalCnt=totalCnt
		.then ()-> toSyncArr
		.then (toSyncArr)-> countRecordsForDB(toSyncArr, prodDB, 'prodDB')
		.then (totalCnt)-> prodDbTotalCnt=totalCnt
		.then () -> cntDiff(prodDbTotalCnt,devDbTotalCnt)

		.then ()-> toSyncArr
		.map (indtype)-> syncType indtype

		#if DBs in syc break
		.catch(promiseBreak.end)
		.then (console.log)

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


checkConnection = (db)->
	Promise.resolve()
		.then ()-> db.ping requestTimeout:1000			
		.then (resp)-> console.log("connected")
		.catch (err)-> 
			console.error(err)
			promiseBreak()

getIndeces = (db)->
	Promise.resolve()
		.then ()-> db.cat.indices format: 'json'
		.then (resp) -> resp.filter (indx) -> indx if indx.index[0] isnt '.'
		.catch console.error


getTypes = (indx)-> 
	currentIndx = indx.index
	prodDataMap = {}
	Promise.resolve()
		.then ()-> prodDB.indices.getMapping index:"#{currentIndx}"
		.then (resp)-> prodDataMap[currentIndx] = Object.keys(resp[currentIndx].mappings)
		.then ()-> prodDataMap


countRecordsForDB = (typesToSync, db, nameofdb)->
	Promise.resolve(typesToSync)
		.map (indtype)-> 
			countType(indtype, db)
		.then (cntarr)-> totalCnt = cntarr.sum()
		.tap (totalCnt)-> console.log "You have #{totalCnt} records for selected types in #{nameofdb}"


cntDiff = (prodDbTotalCnt, devDbTotalCnt)->
	Promise.resolve()
		.then () -> 
			if prodDbTotalCnt - devDbTotalCnt is 0
				promiseBreak('Selected types are in sync')
			else
				prodDbTotalCnt - devDbTotalCnt
		.tap (diff)-> console.log "Prod DB is #{diff} records ahead Dev DB for selected types"


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
	
	Promise.resolve()
		#get last date from db
		.then ()-> getLastRecord(devDB, indtype)
		.then (lastRec)->
			if (lastRec)
				typeProps.date = lastRec['_source'].date
			else 
				typeProps.date = 0
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
		.then ()-> db.search
			index: index
			type: type
			body: body
		.then (res) -> res.hits.hits[0]

moveChunk = (props)-> 

	if props.moved is props.totalToMove
		return
	else
		Promise.resolve()
			.then ()-> getChunk(props)
			.then (res) -> prepForInsertion(res.hits.hits)
			.then (formatedChunk) -> writeChunk(formatedChunk)
			.then () -> props.moved += props.chunkSize
			.then  ()-> moveChunk(props)



getChunk = (props)->
	it = props.indtype.split('/')
	index = it[0]
	type = it[1]
	chunkSize = props.chunkSize

	query = bodybuilder()
		.filter('range', 'date','gte': props.date)
		.build()

	Promise.resolve()
		.then ()-> prodDB.search
			index: index
			type: type
			size: chunkSize
			from: props.moved
			query
		.then (res)-> res
		.tap (res)-> 
			props.date = res.hits.hits[res.hits.hits.length - 1]['_source'].date



writeChunk = (data)->
	Promise.resolve(data)
		.then (data)-> devDB.bulk
			body: data
		.then (res)-> res


prepForInsertion = (data)-> 

	newBody=[]
	body = data.map (doc, docindex) ->	
		metaObj = {}
		return Object.keys(doc).forEach (key, index)->
			# console.log 'key: ', key,  'index: ', index, 'doc: ', doc
			if key is '_index'
				metaObj._index = doc['_index']
				metaObj._type = doc['_type']
				metaObj._id = doc['_id']
				newBody.push index:metaObj
			else if key is '_source'
				newBody.push doc[key]
	return newBody

	

module.exports=syncDB