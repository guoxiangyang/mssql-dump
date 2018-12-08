#!/usr/local/bin/node
'use strict'

var Mssql  = require('mssql')
var Getopt = require('node-getopt');
var fs     = require('fs');
var moment = require('moment');

var getopt = new Getopt([
    ['S' , 'server=ARG',   'Server name'],
    ['U' , 'user=ARG',     'Login ID for server'],
    ['p' , 'port=ARG',     'Server port', 1433],
    ['P' , 'password=ARG', 'Password for login ID'],
    ['d' , 'database=ARG', 'Database name'],
    ['f' , 'format=ARG',   'Output format, csv | insert', 'csv'],
    [''  , 'path=ARG',     'Database name'],
    [''  , 'delete',       'Delete dest folder before save'],
    ['h' , 'help',         'Display this help']
]);

getopt.setHelp(
    "Usage: mssql-dump [OPTION]\n" +
	"Dump MSSQL server database.\n" +
	"\n" +
	"[[OPTIONS]]\n"
);
if (process.argv.length == 2) {
    getopt.showHelp();
    process.exit(1);
}
var opt = getopt.parse(process.argv.slice(2));

if (!opt.options.user
    || !opt.options.password
    || !opt.options.server
    || !opt.options.database) {
    console.error("Missing option");
    getopt.showHelp();
    process.exit(1);
}


var config = {
    user:     opt.options.user,
    password: opt.options.password,
    server:   opt.options.server,
    database: opt.options.database,
}
var path = opt.options.path || opt.options.database;
if (opt.options.delete) {
    var files = fs.readdirSync(path);
    if (files) {
	files.forEach(function(file) {
	    fs.unlinkSync(path + '/' + file);
	});
	fs.rmdirSync(path);
    };
}

fs.mkdir(path, {recursive: true}, function (err) {});

var tables = [];

var table_name    = null;
var fd            = null;
var table_columns = null;

function field_to_string(column, value) {
    var type = column.type;
    if (value === null) {
	return "NULL";
    } else if (type === 'Int') {
    } else if (type === 'Float') {
    } else if (type === 'Text') {
	value = value.replace(/\\/g, "\\\\")
	    .replace(/\0/g, "")
	    .replace(/\n/g, "\\n")
	    .replace(/\r/g, "\\r")
	    .replace(/'/g, "\\'")
	    .replace(/"/g, "\\\"");
    } else if (type === 'Real') {
    } else if (type === 'Char') {
    } else if (type === 'NVarChar') {
    } else if (type === 'NText') {
    } else if (type === 'Bit') {
	value = value.toString('base64')
    } else if (type === 'SmallInt') {
    } else if (type === 'VarBinary') {
	value = value.toString('base64');
    } else if (type === 'Image') {
	value = value.toString('base64');
    } else if (type === 'DateTime') {
	value = moment(value).format("YYYY-MM-DD hh:mm:ss");
    } else if (type === 'VarChar') {
	value = value.replace(/\\/g, "\\\\")
	    .replace(/\0/g, "")
	    .replace(/\n/g, "\\n")
	    .replace(/\r/g, "\\r")
	    .replace(/'/g, "\\'")
	    .replace(/"/g, "\\\"");
    }
    return '"' + value + '"';
}

var sql = Mssql.connect(config, function (err) {
    if (err) {
	console.error(err);
    } else {
	console.log("Server connected...");
	const request = new Mssql.Request();
	request.stream = true;

	function dump_table() {
	    table_name = tables.shift();
	    console.log("Dump:", table_name, "...");
	    if (fd) {
		fs.closeSync(fd);
	    }
	    fd = fs.openSync(path + '/' + table_name + '.csv', 'w');
	    request.query("select * from " + table_name);
	}
	// request.query("SELECT name FROM sysobjects WHERE xtype='U' and name='M_邮件列表'");
	request.query("SELECT name FROM sysobjects WHERE xtype='U'");
	request.on('recordset', function (columns) {
	    if (table_name) {
		table_columns = [];;
		for (var key in columns) {
		    table_columns.push(columns[key]);
		}
		table_columns.sort(function (e1, e2) {
		    if (e1.index < e2.index) {
			return -1;
		    }
		    if (e1.index > e2.index) {
			return 1;
		    }
		    return 0;
		});
		var title=[];
		table_columns.forEach(function (e) {
		    e.type = e.type.name;
		    title.push(e.name);
		});
		title = title.join(',');
		fs.writeSync(fd, title + "\n");
		fs.writeFileSync(path + '/' + table_name + '.json', JSON.stringify(table_columns));
	    }
	})
	
	request.on('row', function (row) {
	    if (fd) {
		var s = [];
		for (var i = 0; i < table_columns.length; i++) {
		    var column = table_columns[i];
		    s.push(field_to_string(column, row[column.name]));
		}
		fs.writeSync(fd, s.join(',') + "\n");
	    } else {
		tables.push(row.name)
	    }
	})
	
	request.on('error', function (err) {
	    console.error(err);
	})
	
	request.on('done', result => {
	    if (tables.length === 0) {
		if (fd) {
		    fs.closeSync(fd);
		}
		process.exit();
	    } else {
		dump_table();
	    }
	})
    }
});

