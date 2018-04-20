#!/usr/bin/env node

const Timer = require('nanotimer');
const AWS = require('aws-sdk');
const bluebird = require('bluebird');
const inquirer = require('inquirer');
const vorpal = require('vorpal')();
const moment = require('moment');
const ora = require('ora');
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
let fil = 'v';

function isHealthy(fields) {
	let status = fields.find(item => item.key === STATUS);
	return status ? status.stringValue === HEALTHY : false;
}

function isRunning(fields) {
	let state = fields.find(item => item.key === STATE);
	return state ? state.stringValue === RUNNING : false;
}

function getPipelineList(filter) {
	return dp.listPipelines({}).promise()
		.then(data => data.pipelineIdList)
		.filter(item => item.name && item.name.indexOf('adhoc') >= 0 && item.name.indexOf(filter) >= 0);
}

function getChoices() {
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

function clear() {
	process.stdout.write('\x1Bc');
}

function pretty(str) {
	return JSON.stringify(str, null, '  ');
}

function validate(id) {
	let params = {
		pipelineId: id
	};

	dp.getPipelineDefinition(params, function (err, data) {
		if (err) {
			vorpal.log(err, err.stack);
		} // an error occurred
		else {
			data.pipelineId = id;
			dp.validatePipelineDefinition(data, (err, dt) => {
				if (err) {
					vorpal.log(err, err.stack);
				} // an error occurred
				else {
					vorpal.log(pretty(dt));
					vorpal.ui.redraw.done();
				}
			});
		}
	});
}

function setDate(params, date) {
	let dateIdx = params.parameterValues.findIndex((val) => {
		return val.id === 'myStartDate'
	});

	if (dateIdx >= 0) {
		params.parameterValues[dateIdx].stringValue = date;
		return params;
	}

	return null;
}

function activate(id, startDate) {
	let params = {
		pipelineId: id
	};

	dp.getPipelineDefinition(params, function (err, data) {
		if (err) {
			vorpal.log(err, err.stack);
		} // an error occurred
		else {
			let activateData = {
				pipelineId: id,
				parameterValues: data.parameterValues,
				startTimestamp: new Date()
			};

			activateData = setDate(activateData, startDate);

			dp.activatePipeline(activateData, (err, dt) => {
				if (err) {
					vorpal.log(err, err.stack);
				} else {
					vorpal.log('Successful Activation');
					vorpal.log(pretty(dt));
				}
			});
		}
	});
}

function runIt(pipelineIds) {
	getPipelineDetails(pipelineIds)
		.then(data => getStatus(data))
		.then(data => {
			clear();
			vorpal.log(pretty(data));
			vorpal.ui.redraw.done();
		})
		.catch(err => {
			console.error(err, err.stack);
		});
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

				activate(answers.pipelineId, moment(answers.beginDate).format('YYYY-MM-DD'));

				timer.setInterval(runIt, [
					[answers.pipelineId]
				], '60s', (time) => {
					console.log(time);
				});

				cb();
			});

	});

vorpal
	.command('stop', 'Stops pipeline')
	.action(function (args, callback) {
		this.log('Data Pipeline stopped!');
		callback();
	});

vorpal.delimiter('dprun$');

vorpal.exec('start').then(() => {
	clear();
	vorpal.show();
});
