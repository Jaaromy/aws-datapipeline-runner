#!/usr/bin/env node

let Timer = require('nanotimer');
let AWS = require('aws-sdk');
let bluebird = require('bluebird');
let inquirer = require('inquirer');
let vorpal = require('vorpal')();
let ora = require('ora');
const spinner = ora('Loading unicorns');

inquirer.registerPrompt('datetime', require('inquirer-datepicker-prompt'));

const HEALTHY = 'HEALTHY';
const RUNNING = 'SCHEDULED';
const STATUS = '@healthStatus';
const STATE = '@pipelineState';



let credentials = new AWS.SharedIniFileCredentials({
	profile: 'default'
});
AWS.config.credentials = credentials;
AWS.config.region = 'us-east-1';
AWS.config.setPromisesDependency(bluebird);

let dp = new AWS.DataPipeline();
let fil = 'v3';


function isHealthy(fields) {
	return fields.find(item => item.key === STATUS).stringValue === HEALTHY;
}

function isRunning(fields) {
	return fields.find(item => item.key === STATE).stringValue === RUNNING;
}

function getPipelineList(filter) {
	return dp.listPipelines({}).promise()
		.then(data => data.pipelineIdList)
		.filter(item => item.name && item.name.indexOf('adhoc') >= 0 && item.name.indexOf(filter) >= 0);
}

function getChoices(filter) {
	return getPipelineList(fil)
		.map(item => {
			return {
				name: item.name,
				value: item.id
			};
		});
}

function getPipelineDetails(pipelineIds) {
	return dp.describePipelines({
			pipelineIds: pipelineIds
		}).promise()
		.then(data => data.pipelineDescriptionList);
}

function getStatus(descriptionList) {
	return bluebird.map(descriptionList, (item, idx) => {
		return {
			index: idx,
			pipelineId: item.pipelineId,
			name: item.name,
			isHealthy: isHealthy(item.fields),
			isRunning: isRunning(item.fields)
		};
	})
}



//runIt();
function runIt(pipelineIds) {
	getPipelineDetails(pipelineIds)
		.then(data => getStatus(data))
		.then(data => {
			vorpal.log(JSON.stringify(data, null, '  '));
			vorpal.ui.redraw();
			vorpal.ui.redraw.done();
			vorpal.ui.redraw.clear();
		})
		.catch(err => {
			console.error(err, err.stack);
		});

	// dp.listPipelines({}).promise()
	// 	.then(data => data.pipelineIdList)
	// 	.filter(item => item.name && item.name.indexOf('adhoc') >= 0 && item.name.indexOf(fil) >= 0)
	// 	.map(item => item.id)
	// 	.then(data => dp.describePipelines({
	// 		pipelineIds: data
	// 	}).promise())
	// 	.then(data => data.pipelineDescriptionList)
	// 	.map((item, idx) => {
	// 		return {
	// 			index: idx,
	// 			pipelineId: item.pipelineId,
	// 			name: item.name,
	// 			isHealthy: isHealthy(item.fields),
	// 			isRunning: isRunning(item.fields)
	// 		};
	// 	})
	// 	.then(data => {
	// 		console.log(JSON.stringify(data, '', '  '));
	// 	})
	// 	.catch(err => {
	// 		console.error(err, err.stack);
	// 	});
}

let questions = [{
		type: 'list',
		name: 'pipelineId',
		message: 'Which pipeline will you be backfilling?',
		choices: getChoices
	},
	{
		type: 'datetime',
		name: 'beginDate',
		message: 'Begin Date:',
		format: ['yyyy', '-', 'mm', '-', 'dd']
	},
	{
		type: 'datetime',
		name: 'endDate',
		message: 'End Date:',
		format: ['yyyy', '-', 'mm', '-', 'dd'],
		date: {
			min: '2017-01-01'
		}
	}
];

vorpal.command('start', 'Starts backfill process')
	.action(function (args, cb) {

		inquirer.prompt(questions)
			.then(answers => {
				let timer = new Timer();
				timer.setInterval(runIt, [
					[answers.pipelineId]
				], '5s', (time) => {
					console.log(time);
				});

				cb();
			});

	});

vorpal
	.command('cancel', 'Cancels backfill')
	.action(function (args, callback) {
		this.log('Backfill cancelled!');
		vorpal.ui.cancel();
	});

vorpal.delimiter('backfill$');

vorpal.exec('start').then(data => {
	vorpal.show();
	vorpal.log('Made it');
});

// inquirer.prompt(questions).then(answers => {
// 	let timer = new Timer();
// 	timer.setInterval(runIt, [
// 		[answers.pipelineId]
// 	], '5s', (time) => {
// 		console.log(time);
// 	});

// 	vorpal
// 		.command('cancel', 'Cancels backfill')
// 		.action(function (args, callback) {
// 			this.log('Backfill cancelled!');
// 			vorpal.ui.cancel();
// 		});

// 	vorpal
// 		.delimiter('backfill$')
// 		.show();

// });
