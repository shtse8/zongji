var EventEmitter = require('events').EventEmitter;
var generateBinlog = require('./lib/sequence/binlog');


class ZongJi extends EventEmitter { 
  constructor (mysqlPool, options) {
    super()
    this.mysqlPool = mysqlPool
    this.options = options || {}

    this.tableMap = {};
    this.ready = false;
    this.useChecksum = false;
    // Include 'rotate' events to keep these properties updated
    this.binlogName = null;
    this.binlogNextPos = null;
  }

  async start (options) {
    this.options = options || {}

    this.ctrlConnection = await this.mysqlPool //.getConnection()
    this.ctrlConnection.on('error', this._emitError)
    this.ctrlConnection.on('unhandledError', this._emitError)

    this.connection = await this.mysqlPool.getConnection()
    this.connection.on('error', this._emitError)
    this.connection.on('unhandledError', this._emitError)

    var binlogOptions = {
      tableMap: this.tableMap,
    };

    let checksumEnabled = await this._isChecksumEnabled()
    this.useChecksum = checksumEnabled
    binlogOptions.useChecksum = checksumEnabled

    if (this.options.startAtEnd) {
      let result = await this._findBinlogEnd()
      if(result){
        binlogOptions.filename = result.Log_name;
        binlogOptions.binlogPos = result.File_size;
      }
    }

    // Run asynchronously from _init(), as serverId option set in start()
    if(this.options.serverId !== undefined){
      binlogOptions.serverId = this.options.serverId;
    }

    console.log('this.options', this.options)
    if(('binlogName' in this.options) && ('binlogNextPos' in this.options)) {
      binlogOptions.filename = this.options.binlogName;
      binlogOptions.binlogPos = this.options.binlogNextPos
    }

    console.log('binlogOptions', binlogOptions)
    let binlogStream = await this.connection.createBinlogStream(binlogOptions)
    binlogStream.on('data', (data) => {
      if (data.header.eventType === 31) {
        console.log('data', data)
      }
    })
    console.log('binlogStream', binlogStream)


    // this.binlog = generateBinlog.call(this, binlogOptions);
    // this.ready = true;
    // new this.binlog()
    console.log('start')
  }

  async _isChecksumEnabled () {
    try {
      console.log('_isChecksumEnabled')
      let [results, fields] = await this.ctrlConnection.query('SELECT @@GLOBAL.binlog_checksum AS `checksum`')
      var checksumEnabled = true;
      if (results[0].checksum === 'NONE') {
        checksumEnabled = false;
      }

      if (checksumEnabled) {
        await this.connection.query('SET @master_binlog_checksum = @@global.binlog_checksum');
        console.log('set checksum variable')
      }

      return checksumEnabled
    } catch (error) {
      if(error.toString().match(/ER_UNKNOWN_SYSTEM_VARIABLE/)){
        // MySQL < 5.6.2 does not support @@GLOBAL.binlog_checksum
      } else {
        // Any other errors should be emitted
        self.emit('error', error);
      }
    }
  }

  async _findBinlogEnd (next) {
    try {
      let [results, fields] = await this.ctrlConnection.query('SHOW BINARY LOGS')
      return results.length > 0 ? results[results.length - 1] : null
    } catch (error) {
      this.emit('error', error);
    }
  }

  async _fetchTableInfo (tableMapEvent) {
    console.log('_fetchTableInfo')
    let tableInfoQueryTemplate = 'SELECT ' +
      'COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, ' +
      'COLUMN_COMMENT, COLUMN_TYPE ' +
      "FROM information_schema.columns " + "WHERE table_schema='%s' AND table_name='%s'";
    let sql = util.format(tableInfoQueryTemplate,
      tableMapEvent.schemaName, tableMapEvent.tableName);
    try {
      let [results, fields] = this.ctrlConnection.query(sql)
      if (results.length === 0) {
        // This is a fatal error, no additional binlog events will be
        // processed since next() will never be called
        throw new Error(
          'Insufficient permissions to access: ' +
          tableMapEvent.schemaName + '.' + tableMapEvent.tableName);
      }

      this.tableMap[tableMapEvent.tableId] = {
        columnSchemas: rows,
        parentSchema: tableMapEvent.schemaName,
        tableName: tableMapEvent.tableName
      };
    } catch (error) {
      this.emit('error', error);
    }
  }


  async stop () {
    // Binary log connection does not end with destroy()
    this.connection.destroy();
    await this.ctrlConnection.query('KILL ' + this.connection.threadId)
    self.ctrlConnection.destroy();
  }

  _skipEvent (eventName) {
    var include = this.options.includeEvents;
    var exclude = this.options.excludeEvents;
    return !(
     (include === undefined ||
      (include instanceof Array && include.indexOf(eventName) !== -1)) &&
     (exclude === undefined ||
      (exclude instanceof Array && exclude.indexOf(eventName) === -1)));
  }

  _skipSchema (database, table) {
    var include = this.options.includeSchema;
    var exclude = this.options.excludeSchema;
    return !(
     (include === undefined ||
      (database !== undefined && (database in include) &&
       (include[database] === true ||
        (include[database] instanceof Array &&
         include[database].indexOf(table) !== -1)))) &&
     (exclude === undefined ||
        (database !== undefined &&
         (!(database in exclude) ||
          (exclude[database] !== true &&
            (exclude[database] instanceof Array &&
             exclude[database].indexOf(table) === -1))))));
  }

  _emitError (error) {
    this.emit('error', error)
  }

}

module.exports = ZongJi;
