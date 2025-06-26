const odbc = require('odbc');
const dateFns = require('date-fns');
const { constants, config } = global.modules;
const dbMap = {};

class DBClass {
    constructor(params) {
        let self = this;
        self.pool = null;
        self.connConfig = params.connConfig;
        self.tables = {};
        for (let tableName of Object.values(params.tableNames)) {
            if (tableName == constants.tableNames.Field || tableName == constants.tableNames.Form ||
                tableName == constants.tableNames.Rule || tableName == constants.tableNames.Record ||
                tableName == constants.tableNames.AutomationConnector || tableName == constants.tableNames.AutomationManager ||
                tableName == constants.tableNames.DataSourceDef || tableName == constants.tableNames.DataSource ||
                tableName == constants.tableNames.CronSchedule || tableName == constants.tableNames.ExcelCluster ||
                tableName == constants.tableNames.TaskAction || tableName == constants.tableNames.AISpaces
            ) {
                continue;
            }
            self.addTable(tableName);
        }
    }

    async checkPassword(password) {
        let self = this;
        let rslt;

        let { connConfig } = self;
        let { driver, host, database, user } = connConfig;
        let connectionString = `Driver=${driver};Server=${host};Database=${database};UID=${user};PWD=${password}`;
        let pool = await odbc.pool(connectionString);

        try {
            let client = await pool.connect();
            rslt = { rc: 0 };
            client.close();
        } catch (err) {
            rslt = { rc: 1, msg: err.code + ' ' + err.message, err: err };
        }
        return rslt;
    }

    async connect() {
        let self = this;
        let rslt;

        let { connConfig } = self;
        let { driver, host, database, user, password, trusted_connection, encrypt } = connConfig;
        let connectionString = `Driver=${driver};Server=${host};Database=${database};UID=${user};PWD=${password};Trusted_Connection=${trusted_connection};Encrypt=${encrypt}`;
        //let connectionString = `Driver=${driver};Server=${host};Database=${database};UID=${user};PWD=${password};Trusted_Connection=no;Encrypt=no`;

        if (self.pool == null) {
            self.pool = await odbc.pool(connectionString);
        }
        let pool = self.pool;

        let client;
        try {
            client = await pool.connect();
            await client.setIsolationLevel(odbc.SQL_TXN_READ_COMMITTED);
            rslt = { rc: 0, data: client };
        } catch (err) {
            if (err.code == '3D000') {
                rslt = await self.createDatabase();
                if (rslt.rc == 0) {
                    rslt = await self.connect();
                } else {
                    rslt = { rc: 1, msg: err.code + ' ' + err.message, err: err };
                }
            } else {
                rslt = { rc: 1, msg: err.code + ' ' + err.message, err: err };
            }
        }

        return rslt;
    }

    async createDatabase() {
        return { rc: 1, msg: 'Not implemented' };
    }

    async query(sql, binding, originalClient) {
        let self = this;
        let rslt;
        let client = originalClient;
        if (client == null) {
            rslt = await self.connect();
            if (rslt.rc != 0) return rslt;
            client = rslt.data;
        }

        binding = binding || [];
        sql = sql.replaceAll('= true', '= 1');
        for (let i = 0; i < binding.length; i++) {
            let val = binding[i];
            if (val === undefined) {
                binding[i] = null;
            } else if (val === true) {
                binding[i] = 1;
            } else if (val === false) {
                binding[i] = 0;
            } else if (val instanceof Date) {
                binding[i] = dateFns.format(val, 'yyyy-MM-dd HH:mm:ss');
            } else if (val instanceof Uint8Array) {
                binding[i] = val.buffer;
            }
            //sql = sql.replace('$' + (i + 1), '?');
        }
        rslt = self.changeSQLParamsNamedToPositional(sql, binding);
        sql = rslt.sql;
        binding = rslt.binding;
        try {
            let data = await client.query(sql, binding);
            rslt = { rc: 0, data: data };
        } catch (err) {
            let msg = err.message;
            if (err.odbcErrors != null && Array.isArray(err.odbcErrors)) {
                for (let odbcError of err.odbcErrors) {
                    msg += '\r\n' + odbcError.code + ' ' + odbcError.message;
                }
            }
            if (msg.indexOf(' already exists ') == -1 && msg.indexOf('There is already an object') == -1) console.error(sql);
            rslt = { rc: 1, msg: msg, err: err };
        } finally {
            if (originalClient == null) client.close();
        }

        return rslt;
    }

    async beginTransaction() {
        let self = this;
        let rslt = await self.connect();
        if (rslt.rc != 0) return rslt;
        let client = rslt.data;

        try {
            await client.beginTransaction();
            rslt = { rc: 0, data: client };
        } catch (err) {
            client.close();
            rslt = { rc: 1, msg: err.code + ' ' + err.message, err: err };
        }

        return rslt;
    }

    async commit(client) {
        let rslt;
        try {
            await client.commit();
            rslt = { rc: 0 };
        } catch (err) {
            rslt = { rc: 1, msg: err.code + ' ' + err.message, err: err };
        } finally {
            client.close();
        }
        return rslt;
    }

    async rollback(client) {
        let rslt;
        try {
            await client.rollback();
            rslt = { rc: 0 };
        } catch (err) {
            rslt = { rc: 1, msg: err.code + ' ' + err.message, err: err };
        } finally {
            client.close();
        }
        return rslt;
    }

    async runWithinTransaction(executeFunction) {
        let self = this;
        let rslt = await self.beginTransaction();
        if (rslt.rc != 0) return rslt;
        let client = rslt.data;
        let args = Array.prototype.slice.call(arguments);
        args.shift();

        try {
            rslt = await executeFunction(client, ...args);
        } catch (err) {
            rslt = { rc: 1, msg: err.code + ' ' + err.message, err: err };
        }

        let finalRslt = rslt;
        if (rslt.rc == 0) {
            rslt = await self.commit(client);
        } else {
            rslt = await self.rollback(client);
        }
        if (rslt.rc != 0) return rslt;
        return finalRslt;
    }

    async retrieve(sql, where, client, limit) {
        let self = this;
        sql = sql.split(' Limit ')[0];
        sql = sql.replace('active = true', 'active = 1');
        let binding = [];
        if (where) {
            if (Array.isArray(where)) {
                binding = where;
            } else {
                let clauses = [];
                let cols = Object.keys(where);
                for (let i = 0; i < cols.length; i++) {
                    let col = cols[i];
                    clauses.push(col + ' = $' + (i + 1));
                    binding.push(where[col]);
                }
                sql += ' Where ' + clauses.join(' And ');
            }
        }

        let rslt = await self.query(sql, binding, client);
        if (rslt.rc != 0) return rslt;
        let rows = self.normalizeRetrievedData(rslt.data);
        return { rc: 0, data: rows };
    }

    changeSQLParamsNamedToPositional(sql, binding){
        let regexp = /\$[0-9]{1,3}/g;
        let matches = [...sql.matchAll(regexp)];
        let newBinding = [];
        matches.forEach((match) => {
            let paramName = match.toString();
            let paramId = parseInt(paramName.substr(1));
            sql = sql.replace(paramName, '?');
            let val = binding[--paramId]; 
            newBinding.push(val);
        });
        return {rc:0, sql:sql, binding: newBinding};
    }

    normalizeRetrievedData(retrievedData) {
        let rows = [];
        let columns = retrievedData.columns;
        for (let i = 0; i < retrievedData.count; i++) {
            let row = retrievedData[i];
            delete row.ss_time_stamp;
            delete row.time_stamp;
            for (let column of columns) {
                if (row[column.name] == null) continue;
                if (column.dataType == 93) {
                    let val = row[column.name];
                    val = dateFns.parse(val.split('.')[0], 'yyyy-MM-dd HH:mm:ss', new Date());
                    row[column.name] = val;
                } else if (column.dataType == -7) {
                    row[column.name] = (row[column.name] == '1');
                } else if (column.dataType == -3) {
                    row[column.name] = new Uint8Array(row[column.name]);
                }
            }
            rows.push(row);
        }
        return rows;
    }

    async retrieveSingle(sql, where, client) {
        let self = this;
        let rslt = await self.retrieve(sql, where, client, 1);
        if (rslt.rc != 0) return rslt;
        let row;
        let rows = rslt.data;
        if (rows.length > 0) row = rows[0];
        return { rc: 0, data: row };
    }

    async insert(tname, data, client) {
        /*if (tname == 'rt_error') {
            return { rc: 0 };
        }*/
        let self = this;
        let cols = Object.keys(data);
        let sql = 'Insert Into ' + tname + '(' + cols.join(', ') + ') Output Inserted.id Values(';
        if (data.error_data != null) {
            data.error_data = JSON.stringify(data.error_data);
        }
        let binding = [];
        for (let i = 0; i < cols.length; i++) {
            let col = cols[i];
            sql += '$' + (i + 1);
            sql += (i < cols.length - 1) ? ', ' : ')';
            let val = data[col];
            if (val != null && val.constructor && val.constructor.name == 'Object') val = JSON.stringify(val);
            binding.push(val);
        }

        let rslt = await self.query(sql, binding, client);
        if (rslt.rc != 0) return rslt;

        let id;
        if (rslt.data != null && rslt.data.length > 0 && rslt.data[0] != null) id = rslt.data[0].id;
        if (id == null && data != null) id = data.id;
        let row;
        if (id != null) {
            rslt = await self.retrieve(`Select * From ${tname} Where id = $1`, [id], client);
            if (rslt.rc != 0) return rslt;
            row = rslt.data[0];
        }

        return { rc: 0, data: row };
    }

    async update(tname, data, where, client) {
        let self = this;
        delete data.id;
        let cols = Object.keys(data);
        let sql = 'Update ' + tname + ' Set ';
        let binding = [];
        for (let i = 0; i < cols.length; i++) {
            let col = cols[i];
            sql += col + ' = $' + (i + 1);
            sql += (i < cols.length - 1) ? ', ' : ' Where ';
            let val = data[col];
            if (val != null && val.constructor && val.constructor.name == 'Object') val = JSON.stringify(val);
            binding.push(val);
        }

        let offset = cols.length + 1;
        cols = Object.keys(where);
        for (let i = 0; i < cols.length; i++) {
            let col = cols[i];
            sql += col + ' = $' + (i + offset);
            sql += (i < cols.length - 1) ? ' And ' : ' ';
            let val = where[col];
            binding.push(val);
        }

        let rslt = await self.query(sql, binding, client);
        if (rslt.rc != 0) return rslt;

        let row = rslt.data;
        let id;
        if (where != null) id = where.id;
        if (rslt.data.count > 0 && id != null) {
            rslt = await self.retrieve(`Select * From ${tname} Where id = $1`, [id], client);
            if (rslt.rc != 0) return rslt;
            row = rslt.data[0];
        }
        return { rc: 0, data: row };
    }

    async del(tname, where, client) {
        let self = this;
        let sql;
        let binding = [];
        let cols = Object.keys(where || {});
        if (cols.length == 0) {
            sql = 'Delete From ' + tname;
        } else {
            sql = 'Delete From ' + tname + ' Where ';

            for (let i = 0; i < cols.length; i++) {
                let col = cols[i];
                sql += col + ' = $' + (i + 1);
                sql += (i < cols.length - 1) ? ' And ' : '';
                binding.push(where[col]);
            }
        }
        sql += ' Select @@RowCount as count';

        let rslt = await self.query(sql, binding, client);
        if (rslt.rc != 0) return rslt;

        return { rc: 0, data: rslt.data.count };
    }

    async updateSequence(tname, min, max) {
        let self = this;
        let rslt;
        let sql;
        let lastId;

        sql = 'Select max(id) From ' + tname + ' Where id >= ' + min + ' And id <= ' + max;
        rslt = await self.retrieveSingle(sql);
        if (rslt.rc != 0) return rslt;
        let newLastId = (rslt.data == null || rslt.data.max == null) ? 0 : Number(rslt.data.max);
        if (newLastId < min) newLastId = min;
        if (lastId == newLastId) return rslt;

        sql = `dbcc checkident (${tname}, reseed, ${newLastId})`;
        rslt = await self.query(sql);
        if (rslt.rc != 0) return rslt;

        return { rc: 0 };
    }

    addTable(tableName) {
        let self = this;
        let tableObj = self.buildTableObj(tableName);
        self.tables[tableName] = tableObj;
        self[tableName] = tableObj;
    }

    buildTableObj(tableName) {
        let tableObj = {};
        let self = this;

        tableObj.retrieve = async function (where, client) {
            return await self.retrieve('Select * From ' + tableName, where, client);
        };

        tableObj.retrieveSingle = async function (where, client) {
            return await self.retrieveSingle('Select * From ' + tableName, where, client);
        };

        tableObj.insert = async function (data, client) {
            return await self.insert(tableName, data, client);
        };

        tableObj.update = async function (data, where, client) {
            return await self.update(tableName, data, where, client);
        };

        tableObj.delete = async function (where, client) {
            return await self.del(tableName, where, client);
        };
        return tableObj;
    }

    getDb(params) {
        let { name, connConfig, tableNames } = params;
        if (name == null) name = 'main';
        if (dbMap[name] != null) return dbMap[name];
        let db = new DBClass({ connConfig: connConfig, tableNames: tableNames });
        dbMap[name] = db;
        return db;
    }

    getArchiveDb() {
        let name = 'archive_db';
        if (dbMap[name] != null) return dbMap[name];
        let db = new DBClass({ connConfig: config.archive_db, tableNames: constants.archiveTableNames });
        dbMap[name] = db;
        return db;
    }

    releaseConnection(client){
        client.close();
    }

}

dbMap.main = new DBClass({
    connConfig: config.db,
    tableNames: constants.tableNames
});

module.exports = dbMap.main;