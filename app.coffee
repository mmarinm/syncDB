require('dotenv').config();
global.ENV = process.env
require('clarify')
global.Promise = require("bluebird")

Sugar = require 'sugar'
Sugar.extend()

syncDB = require('./sync')


#possible arguments
# template: 'lead'
# types: ['campaign/serverclick', 'campaign/clientclick']

syncDB()
