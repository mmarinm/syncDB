promiseBreak = require('promise-break')
elasticsearch = require('elasticsearch')
Ora = require('ora')
axios = require('axios')
bodybuilder = require('bodybuilder')
_progress = require('cli-progress')

# bar = new _progress.Bar(barsize: 65, _progress.Presets.shades_classic)

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
		.then ()-> checkArgs options
		.then (toSync)-> toSyncArr=toSync
		.then ()-> 
			spinner.succeed('start syncing devDB with prodDB')
			spinner.start()
		.then ()-> checkConnection prodDB, 'prodDB'
		.then ()-> 
			spinner.succeed('connection established with prodDB')
			spinner.start()
		.then ()-> checkConnection devDB, 'devDB'
		.then ()-> 
			spinner.succeed('connection established with devDB')
			spinner.start()
		.then ()-> toSyncArr
		.then (toSyncArr) -> checkIndices(toSyncArr)
		.then ()-> toSyncArr
		.mapSeries (indtype)-> syncType indtype
		.then ()->
			spinner.succeed('Everything is in sync now')
			# bar.stop()
		.catch (err)->
			spinner.fail(err)
			# console.error(err)
			promiseBreak.end


checkArgs = (options=optionsDefault)->
	Promise.resolve(options)
		.then (options)-> 
			typesToSync = []
			if options.types or options.template
				for option in options.types or optionsDefault[options.template]
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
		.then (resp)-> 
			spinner.color='green'
			spinner.text="#{tag} connected"


checkIndices = (targets)->
	indices = targets.map((target)-> target.split('/')[0]).unique()
	
	Promise.map indices, (index)->
		Promise.resolve()
			.then ()-> devDB.cat.indices {index}
			.catch ()-> devDB.indices.create {index}


countType = (indtype, db, filter=0)-> 
	[index, type] = indtype.split('/')

	Promise.resolve()
		.then ()-> db.count {index, type}
		.get 'count'


syncType = (indtype)->
	props = {}
	props.indtype = indtype
	props.chunkSize = 5000
	props.moved = 0
	props.lastDate = 0
	type = indtype.split('/')[1]
	
	Promise.resolve()
		.tap ()-> 
			spinner.color='magenta'
			spinner.text ="syncing #{type} type"
		.then ()-> countType(indtype, devDB)
		.tap (count)-> props.totalDevDB = count
		.then (count)-> getLastRecord(devDB, indtype) if count
		.then (lastRec)-> props.lastRecDev = lastRec
		.then ()-> countType(indtype, prodDB)
		.then (count)-> props.totalProdDB = count
		.then ()-> getAmountNeededToSync(props)
		.tap (count)-> 
			if count > 0
				spinner.warn "DevDb is #{count} records behind Prod DB for #{type}"
				spinner.start()
		.then (count)-> props.totalToMove = count
		.then ()-> getLastRecord(prodDB, indtype)
		.then (lastRec)-> props.lastRecProd = lastRec
		# .tap ()-> bar.start(props.totalToMove, 0)
		.then ()-> moveChunk(props)
		.tap ()-> 
			spinner.succeed "#{type} type is in sync"
			spinner.start()

getAmountNeededToSync = (props)->
	return props.totalProdDB if not props.lastRecDev
	[index, type] = props.indtype.split('/')

	body = bodybuilder()
		.filter('range', 'date', gt:props.lastRecDev._source.date)
		.build()

	Promise.resolve()
		.then ()-> prodDB.count {index, type, body}
		.get 'count'

getLastRecord = (db, indtype)-> 
	index = indtype.split('/')[0]
	type = indtype.split('/')[1]
	
	body = bodybuilder()
		.size(1)
		.sort('date', 'desc')
		.build()

	Promise.resolve()
		.then ()-> db.search {index, type, body}
		.then (res) -> res.hits.hits[0]


moveChunk = (props)->
	type = props.indtype.split('/')[1]

	return if props.lastRecDev?._id is props.lastRecProd._id
	if props.moved >= props.totalToMove
		return


	Promise.resolve()
		.then ()-> getChunk(props)
		.then (res)-> prepForInsertion(res.hits.hits)
		.then (body)-> writeChunk(body, props)
		.tap (res)-> props.moved += res.items.length
		.tap ()-> 
			spinner.color='green'
			spinner.text = "#{props.totalToMove - props.moved} records left to sync for #{type} type"
		# .tap ()-> bar.update(props.moved)
		.then ()-> moveChunk(props)


getChunk = (props)->
	it = props.indtype.split('/')
	index = it[0]
	type = it[1]
	
	body = bodybuilder()
	body.filter('range', 'date', gte:props.lastRecDev._source.date) if props.lastRecDev
	body = body.build()
	body.sort = [{date:'asc'}]
	body.search_after = [(new Date(props.lastDateMoved)).valueOf()] if props.lastDateMoved

	Promise.resolve()
		.then ()-> prodDB.search {
			index, type, body
			size: props.chunkSize
		}
		.tap (res)->
			props.lastDateMoved = res.hits.hits[res.hits.hits.length-1]?._source.date or props.lastDateMoved

writeChunk = (data, props)->
	Promise.resolve(data)
		.tap (data)-> 
			if data.length is 0
				spinner.fail('Internal issue with mapping retry')
				promiseBreak()
		.then (body)-> devDB.bulk {body}


prepForInsertion = (data)-> 
	body = []

	for doc in data
		body.push JSON.stringify index:{_index:doc._index, _type:doc._type, _id:doc._id}
		body.push JSON.stringify doc._source

	
	return body.join '\n'

	

module.exports=syncDB