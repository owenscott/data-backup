var fs = require('fs'),
	_ = require('underscore'),
	async = require('async'),
	csvParse = require('csv-parse');

var MongoClient = require('mongodb').MongoClient;

var conf = JSON.parse(fs.readFileSync('./conf.json').toString());
	
MongoClient.connect('mongodb://' + conf.host + ':' + conf.port + '/' + conf.db, function(err, db) {

	if (err) {
		throw err
	}

	var mapKey = function(key) {

		var mapping = {
			TENDERNOTICENUMBER: 'tenderNoticeNumber',
			STATUS: 'status',
			URL: 'documentURL',
			CONTRACTDETAILS: 'contractDetails',
			ISSUER: 'contractIssuer',
			PUBLICATIONDATE: 'publicationDate',
			PUBLISHEDIN: 'publicationVenue',
			DOCUMENTPURCHASEDEADLINE: 'documentPurchaseDeadline',
			SUBMISSIONDEADLINE: 'bidSubmissionDeadline',
			OPENINGDATE: 'bidOpeningDate',
			CONTRACTNAME: 'contractName',
			CONTRACTDESCRIPTION: 'contractDescription',
			COSTESTIMATE: 'costEstimate',
			ESTIMATECURRENCY: 'costEstimateCurrency',
			DATASOURCE: 'dataSource',
			CONTRACTNUMBER: 'contractNumber',
			CONTRACTTYPE: 'contractType',
			PROJECTNAME: 'projectName',
			PROJECTNUMBER: 'projectNumber',
			PROJECTFUNDER: 'projectFunder'
		}

		return mapping[key] || undefined;
	}

	var data = [];

	//GET DATA FROM MONGO
	db.collection('contracts').find().toArray(function(err, rawData) {

		// ----------------------------------------------
		// |       PROCESS RAW DATA TO BETTER JSON      |
		// ----------------------------------------------

		rawData.forEach(function(record) {

			if (record.meta.status === 'closed') {

				var tempRecord = {},
					tempKvps = {},
					locations;

				//translate data to new format

				_.keys(record.data.keyValuePairs.merge).forEach(function(key) {
					if ((record.data.keyValuePairs.merge[key].cleanValue || record.data.keyValuePairs.merge[key].value ) && mapKey(key)) {
						tempKvps[mapKey(key)] = record.data.keyValuePairs.merge[key].cleanValue || record.data.keyValuePairs.merge[key].value;
					}
				})

				//hacky add some other values
				var scrapes = ['STATUS', 'URL', 'ISSUER', 'PUBLICATIONDATE', 'PUBLISHEDIN', 'DOCUMENTPURCHASEDEADLINE', 'SUBMISSIONDEADLINE', 'OPENINGDATE'];

				scrapes.forEach(function(key) {
					if(record.scraped[key]) {
						tempKvps[mapKey(key)] = record.scraped[key];
					}
				})

				//clean data

				if (tempKvps['contractIssuer']) {
					tempKvps['contractIssuer'] = tempKvps['contractIssuer'].replace('Issued by\\u0009','')
				}

				if (tempKvps['contractType'] && tempKvps['contractType'] === 'MULTIPLE TYPES') {
					delete tempKvps['contractType'];
				}

				locations = _.pluck(record.data.arrays.merge.locations,'value');

				//more cleaning data
				
				if (!_.isEmpty(tempKvps)) {
					_.extend(tempRecord, tempKvps);
				}

				if (locations.length > 0) {
					tempRecord.locations = _.clone(locations);
				}


				data.push(_.clone(tempRecord));

			}



		});

		// ----------------------------------------------
		// |      PROCESS RAW DATA TO BETTER JSON      |
		// ----------------------------------------------

		// // ----------------------------------------------
		// // |            PROCESS RAW GADM DATA           |
		// // ----------------------------------------------

		var gadmRaw = [],
			gadm = [],
			i = 0;

		gadmRaw[0] = fs.readFileSync('./ref/gadm-adm1.csv').toString();
		gadmRaw[1] = fs.readFileSync('./ref/gadm-adm2.csv').toString();
		gadmRaw[2] = fs.readFileSync('./ref/gadm-adm3.csv').toString();
		gadmRaw[3] = fs.readFileSync('./ref/gadm-adm4.csv').toString();

		async.eachSeries(gadmRaw, function(csv, callback) {
			csvParse(csv, {columns:true}, function(err, data) {
				gadm[i] = data;
				i++;
				callback();
			})
		},
			function( ){

				var csvOutput = [],
				csvText = '';

				//clean up gadm data
				for (i in gadm) {
					gadm[i] = _.map(gadm[i], function(record) {
						var level = parseInt(i) + 1;
						return {
							level: 'adm' + level,
							name: record['NAME_' + level],
							pid: record.PID,
							type: record['ENGTYPE_' + level],
							levelId: record['ID_' + level]
						}
					});
				}

				// //add gadm data to locations and convert to flat csv output
				data.forEach(function(d) {


					//add gadm data
					d.locations = _.map(d.locations, function(location) {
						var locArr = [];
						var splitLocations = location.split('|');

						for (var i = 0; i < splitLocations.length; i++) {
							if(!_.isEmpty(_.findWhere(gadm[i], {name:splitLocations[i]}))) {
								locArr.push(_.findWhere(gadm[i], {name:splitLocations[i]}))
							}

						}

						return _.flatten(locArr);
						
					})

					d.locations = _.flatten(d.locations);
					d.locations = _.uniq(d.locations, function(d) {return JSON.stringify(d)});

					if (d.locations.length <= 0) {
						delete d.locations;
					}

					if (_.isEmpty(d)) {
						delete d;
					}
			
				})

				fs.writeFileSync('./output/draft-output-for-anjesh-8-31.json', JSON.stringify(data));
				console.log('outputed this many records ', data.length);
			}
		);

		

		db.close();

	})

});

