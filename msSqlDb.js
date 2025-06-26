//const sql = require('mssql');
const odbc = require('odbc');
const dateFns = require('date-fns');

class SqlServerDBClass {
    constructor(params) {
        const self = this;
        self.pool = null;
        self.connConfig = params.connConfig;
    }

    async checkPassword(password) {
        const self = this;
        const { driver, host, database, user } = self.connConfig;
        const connectionString = `Driver=${driver};Server=${host};Database=${database};UID=${user};PWD=${password}`;
        const pool = await odbc.pool(connectionString);

        try {
            const client = await pool.connect();
            client.close();
            return { rc: 0 };
        } catch (err) {
            return { rc: 1, msg: err.code + ' ' + err.message, err: err };
        }
    }

    async connect() {
        const self = this;
        const { driver, host, database, user, password, trusted_connection, encrypt } = self.connConfig;
        const connectionString = `Driver=${driver};Server=${host};Database=${database};UID=${user};PWD=${password};Trusted_Connection=${trusted_connection};Encrypt=${encrypt}`;

        if (self.pool == null) {
            self.pool = await odbc.pool(connectionString);
        }

        let rslt;
        try {
            const client = await self.pool.connect();
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

    async _connect() {
        const self = this;
        const { host, database, user, password, trusted_connection, encrypt } = self.connConfig;

        const connectionConfig = {
            server: host, database, user, password,
            options: { encrypt, trustServerCertificate: trusted_connection }
        };

        if (!self.pool) {
            try {
                // Create a connection pool
                //self.pool = await sql.connect(connectionConfig);
            } catch (err) {
                return { rc: 1, msg: err.code + ' ' + err.message, err: err };
            }
        }

        let rslt;
        try {
            const client = self.pool.request();
            rslt = { rc: 0, data: client };
        } catch (err) {
            rslt = { rc: 1, msg: err.code + ' ' + err.message, err: err };
        }

        return rslt;
    }

    async createDatabase() {
        return { rc: 1, msg: 'Not implemented' };
    }

    async beginTransaction() {
        const self = this;
        let rslt = await self.connect();
        if (rslt.rc != 0) return rslt;
        const client = rslt.data;

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
        const self = this;
        let rslt = await self.beginTransaction();
        if (rslt.rc != 0) return rslt;
        const client = rslt.data;
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

    async query(sql, binding, originalClient) {
        let rslt;
        let self = this;
        let client = originalClient;
        if (!client) {
            rslt = await this.connect();
            if (rslt.rc != 0) return rslt;
            client = rslt.data;
        }

        rslt = self.prepareQuery(sql, binding);
        sql = rslt.sql;
        binding = rslt.binding;

        try {
            let data = await client.query(sql, binding);
            rslt = { rc: 0, data };
        } catch (err) {
            let msg = err.message;
            if (err.odbcErrors && Array.isArray(err.odbcErrors)) {
                for (const odbcError of err.odbcErrors) {
                    msg += `\r\n${odbcError.code} ${odbcError.message}`;
                }
            }
            if (!msg.includes(' already exists ') && !msg.includes('There is already an object')) {
                console.error(sql);
            }
            rslt = { rc: 1, msg, err };
        } finally {
            if (!originalClient) await client.close();
        }

        return rslt;
    }

    prepareQuery(sql, binding = []) {
        // Replace boolean expressions and null checks in SQL (case-insensitive)
        sql = sql.replace(/= true/gi, '= 1').replace(/= false/gi, '= 0')
            .replace(/!= true/gi, '!= 1').replace(/!= false/gi, '!= 0')
            .replace(/<> true/gi, '<> 1').replace(/<> false/gi, '<> 0')
            .replace(/= null/gi, 'IS NULL').replace(/!= null/gi, 'IS NOT NULL');

        const regexp = /\$[0-9]{1,3}/g;
        const uniquePlaceholders = [...new Set(sql.match(regexp))];
        let newSql = sql;
        const newBinding = [];

        for (let i = 0; i < uniquePlaceholders.length; i++) {
            const paramName = uniquePlaceholders[i];
            const paramId = parseInt(paramName.substr(1)) - 1;
            newSql = newSql.replace(new RegExp(`\\${paramName},`, 'g'), '?,');  // Replace all occurrences of each unique paramName
            newSql = newSql.replace(new RegExp(`\\${paramName} `, 'g'), '? ');  // Replace all occurrences of each unique paramName
            newSql = newSql.replace(new RegExp(`\\${paramName}\\)`, 'g'), '?)');  // Replace all occurrences of each unique paramName
            newSql = newSql.replace(new RegExp(`\\${paramName}$`, 'g'), `?`);  // Replace all occurrences of each unique paramName
            newSql = newSql.replace(new RegExp(`\\${paramName}\n`, 'g'), `?`);  // Replace all occurrences of each unique paramName

            let val = binding[paramId];
            if (val === undefined) {
                val = null;
            } else if (val === true) {
                val = 1;
            } else if (val === false) {
                val = 0;
            } else if (val instanceof Date) {
                val = dateFns.format(val, 'yyyy-MM-dd HH:mm:ss');
            } else if (val instanceof Uint8Array) {
                val = val.buffer;
            } else if (Array.isArray(val)) {
                val = val.join(','); // Convert arrays to comma-separated string
            } else if (typeof val === 'object') {
                val = JSON.stringify(val); // Convert objects to JSON string
            }
            newBinding.push(val);
        }

        return { rc: 0, sql: newSql, binding: newBinding };
    }

    async retrieve(sql, where, client, limit) {
        // Extract and remove any existing LIMIT clause
        const limitMatch = sql.match(/\s+LIMIT\s+(\d+)/i);
        if (limitMatch) {
            limit = limitMatch[1];
            sql = sql.replace(limitMatch[0], '');
        }

        let binding = [];
        if (where) {
            if (Array.isArray(where)) {
                binding = where;
            } else {
                let clauses = [];
                let cols = Object.keys(where);
                for (let i = 0; i < cols.length; i++) {
                    clauses.push(`${cols[i]} = $${i + 1}`);
                    binding.push(where[cols[i]]);
                }
                sql += ' WHERE ' + clauses.join(' AND ');
            }
        }

        if (limit) {
            sql += ` ORDER BY 1 OFFSET 0 ROWS FETCH NEXT ${limit} ROWS ONLY`;
        }

        let rslt = await this.query(sql, binding, client);
        if (rslt.rc != 0) return rslt;

        let rows = this.normalizeData(rslt.data);
        return { rc: 0, data: rows };
    }

    normalizeData(retrievedData) {
        const rows = [];
        const columns = retrievedData.columns;

        for (let i = 0; i < retrievedData.length; i++) {
            const row = { ...retrievedData[i] };
            delete row.ss_time_stamp;
            delete row.time_stamp;

            for (const column of columns) {
                const colName = column.name;
                let value = row[colName];
                if (value == null) continue;

                if (column.dataType === 93) { // TIMESTAMP
                    value = dateFns.parse(value.split('.')[0], 'yyyy-MM-dd HH:mm:ss', new Date());
                } else if (column.dataType === -7) { // BIT for boolean
                    value = value === '1';
                } else if (column.dataType === -3) { // VARBINARY
                    value = new Uint8Array(value);
                } else if (colName === 'error_data' && value != null) { // Special handling for error_data
                    value = JSON.parse(value);
                } else if (typeof value === 'string' &&
                    (value.startsWith('[') && value.endsWith(']') || value.startsWith('{') && value.endsWith('}'))) { // JSON strings
                    try {
                        value = JSON.parse(value);
                    } catch (e) {
                        // If parsing fails, keep the value as is
                        continue;
                    }
                }
                row[colName] = value;
            }

            rows.push(row);
        }

        return rows;
    }

    async retrieveSingle(sql, where, client) {
        let rslt = await this.retrieve(sql, where, client, 1);
        if (rslt.rc != 0) return rslt;
        const row = (rslt.data.length > 0) ? rslt.data[0] : null;
        return { rc: 0, data: row };
    }

    async insert(tname, data, client) {
        let self = this;
        const cols = Object.keys(data);
        const placeholders = [];
        const binding = [];

        for (let i = 0; i < cols.length; i++) {
            placeholders.push(`$${i + 1}`);
            binding.push(data[cols[i]]);
        }

        const sql = `INSERT INTO ${tname} (${cols.join(', ')}) OUTPUT Inserted.* VALUES (${placeholders.join(', ')})`;
        let rslt = await this.query(sql, binding, client);
        if (rslt.rc != 0) return rslt;

        const rows = this.normalizeData(rslt.data);
        return { rc: 0, data: rows[0] };
    }

    async update(tname, data, where, client) {
        let self = this;
        delete data.id;

        const dataCols = Object.keys(data);
        const whereCols = Object.keys(where);
        const setClauses = [];
        const whereClauses = [];
        const binding = [];

        for (let i = 0; i < dataCols.length; i++) {
            setClauses.push(`${dataCols[i]} = $${i + 1}`);
            binding.push(data[dataCols[i]]);
        }

        for (let i = 0; i < whereCols.length; i++) {
            whereClauses.push(`${whereCols[i]} = $${dataCols.length + i + 1}`);
            binding.push(where[whereCols[i]]);
        }

        const sql = `UPDATE ${tname} SET ${setClauses.join(', ')} OUTPUT Inserted.* WHERE ${whereClauses.join(' AND ')}`;
        let rslt = await this.query(sql, binding, client);
        if (rslt.rc != 0) return rslt;

        const rows = this.normalizeData(rslt.data);
        return { rc: 0, data: rows[0] };
    }

    async del(tname, where, client) {
        const cols = Object.keys(where || {});
        const binding = [];
        let sql = `DELETE FROM ${tname}`;

        if (cols.length > 0) {
            const whereClauses = [];
            for (let i = 0; i < cols.length; i++) {
                whereClauses.push(`${cols[i]} = $${i + 1}`);
                binding.push(where[cols[i]]);
            }
            sql += ` WHERE ${whereClauses.join(' AND ')}`;
        }

        sql += '; SELECT @@ROWCOUNT AS count';
        let rslt = await this.query(sql, binding, client);
        if (rslt.rc != 0) return rslt;

        return { rc: 0, data: rslt.data.count };
    }

    async updateSequence(tname, min, max) {
        const self = this;

        // Get the current identity value
        let sql = `SELECT IDENT_CURRENT('${tname}') AS current_identity`;
        let rslt = await self.retrieveSingle(sql);
        if (rslt.rc != 0) return rslt;
        if (rslt.data == null) return rslt;
        let currentIdentity = Number(rslt.data.current_identity);

        // Get the maximum ID within the specified range
        sql = `SELECT MAX(id) AS max_id FROM ${tname} WHERE id >= ${min} AND id <= ${max}`;
        rslt = await self.retrieveSingle(sql);
        if (rslt.rc != 0) return rslt;
        let newMaxId = rslt.data.max_id == null ? 0 : Number(rslt.data.max_id);
        if (newMaxId < min) newMaxId = min;
        if (currentIdentity == newMaxId) return rslt;

        // Reset the identity value
        sql = `DBCC CHECKIDENT ('${tname}', RESEED, ${newMaxId})`;
        rslt = await self.query(sql);
        if (rslt.rc != 0) return rslt;

        return { rc: 0 };
    }

    releaseConnection(client){
        client.close();
    }
}

module.exports = SqlServerDBClass;