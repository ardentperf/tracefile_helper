-- java pipelined function to scan all tracefiles in a directory and get metadata from top 30 lines of each file

CREATE TYPE trace_file_type AS OBJECT
(
  file_name VARCHAR2(400),
  file_time DATE,
  file_bytes NUMBER,
  file_traceid VARCHAR2(100),
  first_time DATE,
  first_action VARCHAR2(100),
  first_module VARCHAR2(100),
  first_service VARCHAR2(100),
  sid NUMBER,
  serial NUMBER,
  error VARCHAR2(400)
);
/

create type trace_file_list as table of trace_file_type;
/

CREATE TYPE trace_file_scan_impl AS OBJECT
(
  key INTEGER,

  STATIC FUNCTION ODCITableStart(sctx OUT trace_file_scan_impl, dir VARCHAR2, regex VARCHAR2)
    RETURN NUMBER
    AS LANGUAGE JAVA
    NAME 'com.ardentperf.trace_file_helper.ODCITableStart(oracle.sql.STRUCT[], java.lang.String, java.lang.String) return java.math.BigDecimal',

  MEMBER FUNCTION ODCITableFetch(self IN OUT trace_file_scan_impl, nrows IN NUMBER,
                                 outSet OUT trace_file_list) RETURN NUMBER
    AS LANGUAGE JAVA
    NAME 'com.ardentperf.trace_file_helper.ODCITableFetch(java.math.BigDecimal, oracle.sql.ARRAY[]) return java.math.BigDecimal',

  MEMBER FUNCTION ODCITableClose(self IN trace_file_scan_impl) RETURN NUMBER
    AS LANGUAGE JAVA
    NAME 'com.ardentperf.trace_file_helper.ODCITableClose() return java.math.BigDecimal'
);
/

CREATE FUNCTION trace_file_scan(dir varchar2, regex VARCHAR2) RETURN trace_file_list
PIPELINED USING trace_file_scan_impl;
/


set define off

create and compile java source named trace_file_helper as
package com.ardentperf;
import java.io.*;
import oracle.CartridgeServices.*;
import java.math.BigDecimal;
import oracle.sql.*;
import java.sql.*;
import java.util.*;
import java.lang.*;
import java.text.*;
// stored context type

public class StoredCtx
{
  Iterator files;
  String search;
  public StoredCtx(Iterator f, String s) { files=f; search=s; }
}

public class trace_file_helper implements SQLData
{
  private BigDecimal key;

  final static BigDecimal SUCCESS = new BigDecimal(0);
  final static BigDecimal ERROR = new BigDecimal(1);

  // Implement SQLData interface.

  String sql_type;
  public String getSQLTypeName() throws SQLException
  {
    return sql_type;
  }

  public void readSQL(SQLInput stream, String typeName) throws SQLException
  {
    sql_type = typeName;
    key = stream.readBigDecimal();
  }

  public void writeSQL(SQLOutput stream) throws SQLException
  {
    stream.writeBigDecimal(key);
  }


  // type methods implementing ODCITable interface

  static public BigDecimal ODCITableStart(STRUCT[] sctx,String dir,String regex)
    throws SQLException
  {
    Connection conn = DriverManager.getConnection("jdbc:default:connection:");

    // create a stored context and store the file collection in it
    File topdir=new File(dir);
    StoredCtx ctx=new StoredCtx(Arrays.asList(topdir.listFiles()).iterator(),regex);

    // register stored context with cartridge services
    int key;
    try {
      key = ContextManager.setContext(ctx);
    } catch (CountException ce) {
      return ERROR;
    }

    // create a trace_file_scan_impl instance and store the key in it
    Object[] impAttr = new Object[1];
    impAttr[0] = new BigDecimal(key);
    StructDescriptor sd = new StructDescriptor("TRACE_FILE_SCAN_IMPL",conn);
    sctx[0] = new STRUCT(sd,conn,impAttr);

    return SUCCESS;
  }

  public BigDecimal ODCITableFetch(BigDecimal nrows, ARRAY[] outSet)
    throws SQLException
  {
    Connection conn = DriverManager.getConnection("jdbc:default:connection:");

    // retrieve stored context using the key
    StoredCtx ctx;
    try {
      ctx=(StoredCtx)ContextManager.getContext(key.intValue());
    } catch (InvalidKeyException ik ) {
      return ERROR;
    }

    // get the nrows parameter
    int nrowsval = nrows.intValue();

    // create a vector for the fetched rows
    Vector v = new Vector(nrowsval);
    int i=0;

    StructDescriptor outDesc =
      StructDescriptor.createDescriptor("TRACE_FILE_TYPE", conn);
    Object[] out_attr = new Object[11];

    while(nrowsval>0 && ctx.files.hasNext()){
      File f = (File)ctx.files.next();
      String fn = f.getName();
      if(!fn.matches(ctx.search)) continue;
      out_attr[0] = (Object)fn; // file_name
      out_attr[1] = (Object)new Timestamp(f.lastModified()); // file_time
      out_attr[2] = (Object)new BigDecimal(f.length()); // file_bytes
      String[] fn_s = fn.split("_",4);
      out_attr[3] = (Object)(fn_s.length==4?fn_s[3].substring(0,fn_s[3].length()-4):""); // file_traceid
      
      // now we scan the file...
      BufferedReader r;
      try {
        r=new BufferedReader(new FileReader(f));
        try {
          int nLines=0;
          String line;
          while(nLines++<30 && ((line=r.readLine())!=null)) {
            if(line.startsWith("*** ACTION NAME:")) {
              out_attr[5] = (Object)line.substring(17,line.indexOf(')')); // first_action
            } else if(line.startsWith("*** MODULE NAME:")) {
              out_attr[6] = (Object)line.substring(17,line.indexOf(')')); // first_module
            } else if(line.startsWith("*** SERVICE NAME:")) {
              out_attr[7] = (Object)line.substring(18,line.indexOf(')')); // first_service
            } else if(line.startsWith("*** SESSION ID:")) {
              out_attr[8] = (Object)new BigDecimal(line.substring(16,line.indexOf('.'))); // sid
              out_attr[9] = (Object)new BigDecimal(line.substring(line.indexOf('.')+1,line.indexOf(')'))); // serial
            } else if(line.matches(".* [0-9-]{10} [0-9:.]{12}$")) {
              SimpleDateFormat df= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
              Timestamp t=new Timestamp(df.parse(line.substring(line.length()-23)).getTime());
              out_attr[4] = (Object)t; // first_time
            } else if(line.startsWith("*** TRACE DUMP CONTINUED FROM FILE")) {
              String new_fn = line.substring(35,line.length()-4);
              r.close(); 
              r=new BufferedReader(new FileReader(new_fn));
              nLines=0;
            }
            
          }
        } catch(IOException e) {
          out_attr[10]=e.toString();
        } catch(ParseException e) {
          out_attr[10]=e.toString();
        };

        try {
          r.close();
        } catch(IOException e) {
          out_attr[10]=e.toString();
        };
      } catch(FileNotFoundException e) {
        out_attr[10]=e.toString();
      }
      
      v.add((Object)new STRUCT(outDesc, conn, out_attr));
      i+=1;
      nrowsval-=1;
    }

    // return if no files found
    if(i==0) return SUCCESS;

    // create the output ARRAY using the vector
    Object out_arr[] = v.toArray();
    ArrayDescriptor ad = new ArrayDescriptor("TRACE_FILE_LIST",conn);
    outSet[0] = new ARRAY(ad,conn,out_arr);

    return SUCCESS;
  }

  public BigDecimal ODCITableClose() throws SQLException {

    // retrieve stored context using the key, and remove from ContextManager
    StoredCtx ctx;
    try {
      ctx=(StoredCtx)ContextManager.clearContext(key.intValue());
    } catch (InvalidKeyException ik ) {
      return ERROR;
    }

    // any needed cleanup

    return SUCCESS;
  }
}
/



create view all_trace_files as
select 'UDUMP' directory, t.* from table(trace_file_scan('/u0001/app/oracle/admin/ardentp/udump','ardentp.*')) t
UNION ALL
select 'BDUMP' directory, t.* from table(trace_file_scan('/u0001/app/oracle/admin/ardentp/bdump','ardentp.*')) t;
/



--- permissions

call dbms_java.grant_permission('DEVELOPER', 'java.io.FilePermission', '/u0001/app/oracle/admin/ardentp/udump', 'read');
call dbms_java.grant_permission('DEVELOPER', 'java.io.FilePermission', '/u0001/app/oracle/admin/ardentp/bdump', 'read');
create directory UDUMP as '/u0001/app/oracle/admin/ardentp/udump';
create directory BDUMP as '/u0001/app/oracle/admin/ardentp/bdump';
grant read on directory UDUMP to DEVELOPER;
grant read on directory BDUMP to DEVELOPER;
