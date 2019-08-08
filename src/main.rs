#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;

use std::env;


use cdrs::authenticators::{StaticPasswordAuthenticator};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;

use cdrs::frame::IntoBytes;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<StaticPasswordAuthenticator>>>;

fn main() {
  let user = "user";
  let password = "password";
  let auth = StaticPasswordAuthenticator::new(&user, &password);
  let node = NodeTcpConfigBuilder::new("139.99.221.130:9042", auth).build();
  let cluster_config = ClusterTcpConfig(vec![node]);
  let no_compression: CurrentSession =
    new_session(&cluster_config, RoundRobin::new()).expect("session should be created");
   let enable_table = env::var("USE_TNS").unwrap_or("users".to_string()).to_string();
  let create = env::var("CREATE").unwrap_or("nope".to_string()).to_string();
  create_keyspace(&no_compression);
  //create_udt(&no_compression);
  if create != "nope".to_string() {
    create_table(&no_compression);
    create_table_tnx(&no_compression);
  }

let mut table = "users_tnx".to_string();
  if enable_table == "1".to_string() {
      table = "users".to_string();
  }
  //
  select_struct(&no_compression , table);
  //update_struct(&no_compression);
  //delete_struct(&no_compression);
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RowStruct {
  key: i32,
  username: String,
  firstname: String,
  lastname: String,
  email : String
}

impl RowStruct {
  fn into_query_values(self) -> QueryValues {
    query_values!("key" => self.key, "username" => self.username, "firstname" => self.firstname, "lastname" => self.lastname, "email" => self.email)
  }
}

#[derive(Debug, Clone, PartialEq, IntoCDRSValue, TryFromUDT)]
struct User {
  username: String,
}

fn create_keyspace(session: &CurrentSession) {
  let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
  session.query(create_ks).expect("Keyspace creation error");
}


fn create_table(session: &CurrentSession) {
  let create_table_cql =
    "CREATE TABLE IF NOT EXISTS test_ks.users (key int PRIMARY KEY, username text, firstname text, lastname text, email text) WITH transactions = { 'enabled' : true };";
  session
    .query(create_table_cql)
    .expect("Table creation error");
    insert_struct(&session, "users".to_string());
}

fn create_table_tnx(session: &CurrentSession) {
  let create_table_cql =
    "CREATE TABLE IF NOT EXISTS test_ks.users_tnx (key int PRIMARY KEY, username text, firstname text, lastname text, email text);";
  session
    .query(create_table_cql)
    .expect("Table creation error");
    insert_struct(&session, "users_tnx".to_string());
}

fn insert_struct(session: &CurrentSession, table_name : String ) {
  for i in 0..10 {
  let row = RowStruct {
    key: i as i32,
    username: format!("John-user-{}", 1).to_string(),
    firstname: format!("John-{}", 1).to_string(),
   lastname :  format!("Tester-user-{}", 1).to_string(),
   email :  format!("john{}@nwh.com", 1).to_string(),
  };

  let insert_struct_cql = format!("INSERT INTO test_ks.{} (key, username, firstname, lastname, email) VALUES (?, ?, ?, ?,?)", table_name).to_string();
  session
    .query_with_values(insert_struct_cql, row.into_query_values())
    .expect("insert");
  }
}

fn select_struct(session: &CurrentSession, table_name : String ) {
  let select_struct_cql = format!("SELECT * FROM test_ks.{}", table_name).to_string();
  let rows = session
    .query(select_struct_cql)
    .expect("query")
    .get_body()
    .expect("get body")
    .into_rows()
    .expect("into rows");

  for row in rows {
    let my_row: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
    println!("struct got: {:?}", my_row);
  }
}
