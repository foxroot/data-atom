"use babel";
global._ = require('underscore.string')
var a=''
var b='CM_DAN_JACKSON'
var c='PROD_CRM_ACCESS_WH'
var f='pets'
var g='eu-central-1'
var snowflake = require('snowflake-sdk');
var connection = snowflake.createConnection({
  account: f,
  username: b,
  password: a,
  warehouse:c,
  region: g
});
class DataManager{
  constructor(connectionName) {
    this.connectionName = connectionName;
  }
  connect(){
       return connection.connect(function(err, conn) {
        if (err) {
          console.error('Unable to connect: ' + err.message);
        } else {
          console.log('Successfully connected as id: ' + connection.getId());
        }
      });
  }
  execute(query,onQueryToken){
    return new Promise((resolve, reject) => {
         global.snowquery=connection.execute({sqlText: query,
      complete: function(err, stmt, rows) {
        if (err) {
        console.error('Failed to execute statement due to the following error: ' + err.message);
        reject(err);
        } else {
          console.log(rows);
          console.log(typeof(rows));
        //  resolve(rows);
        resolve(rows);
        }
      }
    });
    if (onQueryToken) {
    //   give back some data so they can tell us to cancel the query if needed
      //onQueryToken({connection: connection, execute: snowquery});
    return onQueryToken;
    }
  });
}
//  connection.cancel(queryToken.connection.connectionParameters, queryToken.connection, queryToken.query);
//}
cancelExecution(queryToken) {
snowquery.cancel(function(err, stmt) {
  if (err) {
    console.error('Unable to abort statement due to the following error: ' + err.message);
  } else {
    console.log('Successfully aborted statement');
  }
});
  }

getDatabaseNames() {
  return new Promise((resolve, reject) => {
      var query = 'show databases';
      if (!connection) {
        console.error(err);
        return reject(err);
      }
      connection.execute({sqlText:query,
      complete: function(err, stmt, rows) {
        if (err) {
          console.error(err);
          return reject(err);
        }
else{
        var s=rows.length
        for(var i=0;i < s;i++){
          var ss=JSON.stringify(rows[i])
        var array = ss.split(',');
        var name=array[1];
        var arr=name.split(':');
        console.log(arr[1].slice(1, -1));
        }
        resolve(rows);
      }
      }
    });
});
}
getTables(database) {
  return new Promise((resolve, reject) => {
  var query='SELECT table_schema, table_name, table_type FROM '+ database +'.information_schema.tables order by table_schema, table_type, table_name;'
  if (!connection) {
    console.error(err);
    return reject(err);
  }
  connection.execute({sqlText: query,
    complete: function(err, stmt, rows) {
      if (err) {
      console.error('Failed to execute statement due to the following error: ' + err.message);
      return reject(err);
      } else {
        var s=rows.length;
        var tables = [];
        for(var i=0;i < s;i++){
          var ss=JSON.stringify(rows[i])
var array=ss.split(',');
var schema=array[0]
if( schema!='{"TABLE_SCHEMA":"INFORMATION_SCHEMA"')
{
tables.push({
schema:array[0].split(':')[1].slice(1,-1),
name:array[1].split(':')[1].slice(1,-1),
type:array[2].split(':')[1].slice(1, -2) === 'BASE TABLE' ? 'Table' : 'View'
});
}
      }
      console.log(tables);
      return tables;
      resolve(rows);
    }
    }
  });
  });
  }
  getTableDetails(database, tables) {
  return new Promise((resolve, reject) => {
   var sqlTables = "('" + tables.join("','") +  "')";
   var query = 'SELECT column_name, data_type, character_maximum_length, table_name from '+ database + '.information_schema.COLUMNS WHERE table_name IN '+ sqlTables + 'ORDER BY table_name, ordinal_position';
   connection.execute({sqlText: query,
     complete: function(err, stmt, rows) {
       if (err) {
       console.error('Failed to execute statement due to the following error: ' + err.message);
       return reject(err);
       } else {
         var s=rows.length
         var columns=[]
         for(var i=0;i < s;i++){
           var ss=JSON.stringify(rows[i])
var array=ss.split(',');
 columns.push({
 name:array[0].split(':')[1].slice(1,-1),
 type:array[1].split(':')[1].slice(1,-1),
 size:array[2].split(':')[1],
 tableName:array[3].split(':')[1].slice(1, -2)
});

       }
       console.log(columns);
       return columns;
       resolve(rows);
     }
     }
   });
   });
 }
 getTableQuery(table) {
   return 'SELECT * FROM ' + table + ' LIMIT 100;';
 }
 getDefaultSchema() {
    return 'public';
  }
  getSchemas(database) {
    var query='SELECT schema_name FROM '+ database + '.information_schema.schemata;'
    connection.execute({sqlText: query,
      complete: function(err, stmt, rows) {
        if (err) {
        console.error('Failed to execute statement due to the following error: ' + err.message);
        } else {
          var s=rows.length;
          var items = [];
          for(var i=0;i < s;i++){
            var ss=JSON.stringify(rows[i]);
  var array=ss.split(',');
  items.push({
  name:array[0].split(':')[1].slice(1,-2),
  type:'Schema'
  });
        }
        console.log(items);
        return items;
      }
      }
    });
    }
  destroy(){
  return  connection.destroy(function(err, conn) {
      if (err) {
        console.error('Unable to disconnect: ' + err.message);
      } else {
        console.log('Disconnected connection with id: ' + connection.getId());
      }
    });
  }
}
module.exports=DataManager;
const ha=new DataManager(connection);
console.log(ha.connect());
//console.log(ha.execute('select *  from PROD_PETS_MAIN_AWS_REPLAY.PETS_MAIN_AWS_REPLAY.gi_brand;',2));
//console.log(ha.cancelExecution(2));
//console.log(ha.getDatabaseNames());
//console.log(ha.getTables('PROD_PETS_MAIN_AWS_REPLAY'));
//console.log(ha.getTableDetails('PROD_PETS_MAIN_AWS_REPLAY',['APPLICABLE_ROLES','META']));
//console.log(ha.getTableQuery('PROD_PETS_MAIN_AWS_REPLAY.PETS_MAIN_AWS_REPLAY.DE_APPLICATIONS'));
//console.log(ha.getDefaultSchema());
console.log(ha.getSchemas('PROD_PETS_MAIN_AWS_REPLAY'));
console.log(ha.destroy());
