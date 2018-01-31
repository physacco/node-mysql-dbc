const mysql = require('mysql2/promise');

class RollbackError extends Error {
  constructor(message) {
    super(message);
    this.message = message;
    this.name = 'RollbackError';
  }
}

class Connection {
  static async new(pooler) {
    const conn = await pooler.getPool().getConnection();
    return new Connection(conn);
  }

  static async run(pooler, func, args) {
    const conn = await Connection.new(pooler);

    try {
      return await func.apply(conn, args);
    } finally {
      conn.release();
    }
  }

  constructor(conn) {
    this.conn = conn;
  }

  release() {
    this.conn.release();
  }

  query(sql, args) {
    return this.conn.query(sql, args);
  }

  doSelect(sql, args) {
    return this.conn.query(sql, args).
    then(result => getRows(result));
  }

  doSelectOne(sql, args) {
    return this.doSelect(sql, args).
    then(rows => rows[0] || null);
  }

  doInsert(sql, args) {
    return this.conn.query(sql, args).
    then(result => getInfo(result).insertId);
  }

  doUpdate(sql, args) {
    return this.conn.query(sql, args).
    then(result => getInfo(result).affectedRows);
  }

  doDelete(sql, args) {
    return this.conn.query(sql, args).
    then(result => getInfo(result).affectedRows);
  }

  doBegin() {
    return this.conn.query('BEGIN');
  }

  doCommit() {
    return this.conn.query('COMMIT');
  }

  doRollback() {
    return this.conn.query('ROLLBACK');
  }

  async doTransaction(fn) {
    await this.doBegin();

    try {
      const res = await fn.call(this);
      await this.doCommit();
      return res;
    } catch (err) {
      await this.doRollback();
      if (!(err instanceof RollbackError)) {
        throw err;
      }
    }
  }

  selectAll(table) {
    const sql = `SELECT * FROM ${table}`;
    const args = [];
    return this.doSelect(sql, args);
  }

  selectManyByField(table, field, value) {
    const sql = `SELECT * FROM ${table} WHERE ${field} = ?`;
    const args = [value];
    return this.doSelect(sql, args);
  }

  selectManyByFields(table, fields, values) {
    const fs = fields.map(field => `${field} = ?`).join(' AND ');
    const sql = `SELECT * FROM ${table} WHERE ${fs}`;
    const args = values;
    return this.doSelect(sql, args);
  }

  selectOneByField(table, field, value) {
    const sql = `SELECT * FROM ${table} WHERE ${field} = ? LIMIT 1`;
    const args = [value];
    return this.doSelectOne(sql, args);
  }

  selectById(table, id) {
    return this.selectOneByField(table, 'id', id);
  }

  selectOneByFields(table, fields, values) {
    const fs = fields.map(field => `${field} = ?`).join(' AND ');
    const sql = `SELECT * FROM ${table} WHERE ${fs} LIMIT 1`;
    const args = values;
    return this.doSelectOne(sql, args);
  }

  selectOneByObject(table, info) {
    const fields = Reflect.ownKeys(info);
    const values = fields.map(key => info[key]);
    return this.selectOneByFields(table, fields, values);
  }

  insertOne(table, fields, values) {
    const fs = fields.join(', ');
    const vs = fields.map(field => '?').join(', ');
    const sql = `INSERT INTO ${table} (${fs}) VALUES (${vs})`;
    return this.doInsert(sql, values);
  }

  insertOneObject(table, info) {
    const fields = Reflect.ownKeys(info);
    const values = fields.map(key => info[key]);
    return this.insertOne(table, fields, values);
  }

  insertMany(table, fields, list) {
    let placeholders = [];
    for (let i = 0; i < fields.length; i += 1) {
      placeholders.push('?');
    }

    const fs = `(${fields.join(', ')})`;
    const vs = `(${placeholders.join(', ')})`;

    let sql = `INSERT INTO ${table} ${fs} VALUES `;
    for (let i = 0; i < list.length; i += 1) {
      if (i > 0) {
        sql += ', ';
      }

      sql += vs;
    }

    const args = [].concat.apply([], list);  // flatten
    return this.doInsert(sql, args);
  }

  insertManyObjects(table, objects) {
    const fields = Reflect.ownKeys(objects[0]);
    const list = objects.map(obj => fields.map(key => obj[key]));
    return this.insertMany(table, fields, list);
  }

  updateOne(table, id, fields, values) {
    if (fields.length === 0) {
      throw new Error('no fields to update');
    }

    if (values.length !== fields.length) {
      throw new Error('values.length !== fields.length');
    }

    const fs = fields.map(field => `${field} = ?`).join(', ');
    let sql = `UPDATE ${table} SET ${fs} WHERE id = ?`;

    const args = values.concat(id);
    return this.doUpdate(sql, args);
  }

  updateOneByFields(table, fields, values, whereFields, whereValues) {
    if (fields.length === 0) {
      throw new Error('no fields to update');
    }

    if (values.length !== fields.length) {
      throw new Error('values.length !== fields.length');
    }

    if (whereFields.length !== whereValues.length) {
      throw new Error('where values.length !== where fields.length');
    }

    const fs = fields.map(field => `${field} = ?`).join(', ');
    const whereFs = whereFields.map(field => `${field} = ?`).join(' AND ');

    let sql = `update ${table} set ${fs} where ${whereFs}`;

    const args = values.concat(whereValues);
    return this.doUpdate(sql, args);
  }

  updateOneObject(table, info) {
    const fields = [];
    const values = [];
    for (let key of Reflect.ownKeys(info)) {
      if (key !== 'id' && key !== 'ctime' && key !== 'mtime') {
        fields.push(key);
        values.push(info[key]);
      }
    }

    return this.updateOne(table, info.id, fields, values);
  }

  deleteByField(table, field, value) {
    const sql = `DELETE FROM ${table} WHERE ${field} = ?`;
    const args = [value];
    return this.doDelete(sql, args);
  }

  deleteById(table, id) {
    return this.deleteByField(table, 'id', id);
  }
}

// result is the return value of conn.query
function getRows(result) {
  const [rows, fields] = result;
  return rows;
}

// result is the return value of conn.{insert, update, delete}
function getInfo(result) {
  const [resultSetHeader, unknown] = result;
  return resultSetHeader;
}

function Dbc() {
  this.Pooler = {
    pool: null,

    getPool: function () {
      if (!this.pool) {
        throw new Error('database not connected');
      }

      return this.pool;
    },

    setPool: function (pool) {
      this.pool = pool;
    }
  };
}

/**
 * Initialize the connection pool.
 * @param {Object} info: e.g. {
 *   host: 'localhost',
 *   port: 3306,
 *   user: 'root',
 *   password: 'password',
 *   database: 'db1p'
 * }
 * @returns undefined
 */
Dbc.prototype.init = function (info) {
  const poolConfig = Object.assign({
    connectionLimit: 20,
    queueLimit: 10
  }, info);

  this.Pooler.setPool(mysql.createPool(poolConfig));
};

/**
 * Function wrapper.
 * fn具有以下性质：
 * 1. 是普通函数，不是星号函数
 * 2. 返回一个 Promise 实例
 * 3. this 是一个 Connection 实例
 * 4. 可以用 withConnection 包装
 */
Dbc.prototype.withConnection = function (fn) {
  const pooler = this.Pooler;
  return function () {
    return Connection.run(pooler, fn, arguments);
  };
};

exports.createDbc = function () {
  return new Dbc();
};
