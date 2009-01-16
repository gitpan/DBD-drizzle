/*
   Copyright (c) 2008   Patrick Galbraith

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


#include "dbdimp.h"
#include "constants.h"


DBISTATE_DECLARE;


MODULE = DBD::drizzle	PACKAGE = DBD::drizzle

INCLUDE: drizzle.xsi

MODULE = DBD::drizzle	PACKAGE = DBD::drizzle

double
constant(name, arg)
    char* name
    char* arg
  CODE:
    RETVAL = drizzle_constant(name, arg);
  OUTPUT:
    RETVAL


MODULE = DBD::drizzle	PACKAGE = DBD::drizzle::dr

void
_ListDBs(drh, host=NULL, port=NULL, user=NULL, password=NULL)
    SV *        drh
    char *	host
    char *      port
    char *      user
    char *      password
  PPCODE:
{
  DRIZZLE drizzle;
  DRIZZLE* con= drizzle_dr_connect(drh, &drizzle, NULL, host, port, user, password,
                                   NULL, NULL);
  if (con != NULL)
  {
    DRIZZLE_ROW cur;
    DRIZZLE_RES* res;

    if (drizzle_query(con,"SHOW DATABASES"))
      do_error(drh, drizzle_errno(con), drizzle_error(con), drizzle_sqlstate(con));

    res= drizzle_store_result(con);
    if (!res)
    {
      do_error(drh, drizzle_errno(con), drizzle_error(con), drizzle_sqlstate(con));
    }
    else
    {
      EXTEND(sp, drizzle_num_rows(res));
      while ((cur = drizzle_fetch_row(res)))
      {
        PUSHs(sv_2mortal((SV*)newSVpv(cur[0], strlen(cur[0]))));
      }
      drizzle_free_result(res);
    }
    drizzle_close(con);
  }
}


void _admin_internal(drh,dbh,command,dbname=NULL,host=NULL,port=NULL,user=NULL,password=NULL)
  SV* drh
  SV* dbh
  char* command
  char* dbname
  char* host
  char* port
  char* user
  char* password
  PPCODE:
{
  DRIZZLE drizzle;
  int retval;
  DRIZZLE* con;

  /*
   *  Connect to the database, if required.
 */
  if (SvOK(dbh)) {
    D_imp_dbh(dbh);
    con= imp_dbh->pdrizzle;
  }
  else
  {
    con= drizzle_dr_connect(drh, &drizzle, NULL, host, port, user,  password, NULL, NULL);
    if (con == NULL)
    {
      do_error(drh, drizzle_errno(&drizzle), drizzle_error(&drizzle),
               drizzle_sqlstate(&drizzle));
      XSRETURN_NO;
    }
  }

  if (strEQ(command, "shutdown"))
    retval = drizzle_shutdown(con);
  else if (strEQ(command, "refresh"))
    retval = drizzle_refresh(con, REFRESH_LOG);
  else if (strEQ(command, "createdb"))
  {
    char* buffer = malloc(strlen(dbname)+50);
    if (buffer == NULL)
    {
      do_error(drh, JW_ERR_MEM, "Out of memory" ,NULL);
      XSRETURN_NO;
    }
    else
    {
      strcpy(buffer, "CREATE DATABASE ");
      strcat(buffer, dbname);
      retval = drizzle_real_query(con, buffer, strlen(buffer));
      free(buffer);
    }
  }
  else if (strEQ(command, "dropdb"))
  {
    char* buffer = malloc(strlen(dbname)+50);
    if (buffer == NULL)
    {
      do_error(drh, JW_ERR_MEM, "Out of memory" ,NULL);
      XSRETURN_NO;
    }
    else
    {
      strcpy(buffer, "DROP DATABASE ");
      strcat(buffer, dbname);
      retval = drizzle_real_query(con, buffer, strlen(buffer));
      free(buffer);
    }
  }
  else
  {
    croak("Unknown command: %s", command);
  }
  if (retval)
  {
    do_error(SvOK(dbh) ? dbh : drh, drizzle_errno(con),
             drizzle_error(con) ,drizzle_sqlstate(con));
  }

  if (SvOK(dbh))
  {
    drizzle_close(con);
  }
  if (retval)
    XSRETURN_NO;
  else 
    XSRETURN_YES;
}


MODULE = DBD::drizzle    PACKAGE = DBD::drizzle::db


void
type_info_all(dbh)
  SV* dbh
  PPCODE:
{
  /* 	static AV* types = NULL; */
  /* 	if (!types) { */
  /* 	    D_imp_dbh(dbh); */
  /* 	    if (!(types = dbd_db_type_info_all(dbh, imp_dbh))) { */
  /* 	        croak("Cannot create types array (out of memory?)"); */
  /* 	    } */
  /* 	} */
  /* 	ST(0) = sv_2mortal(newRV_inc((SV*) types)); */
  D_imp_dbh(dbh);
  ST(0) = sv_2mortal(newRV_noinc((SV*) dbd_db_type_info_all(dbh,
                                                            imp_dbh)));
  XSRETURN(1);
}


void
_ListDBs(dbh)
  SV*	dbh
  PPCODE:
  D_imp_dbh(dbh);
  DRIZZLE_RES* res;
  DRIZZLE_ROW cur;
  if (drizzle_query(imp_dbh->pdrizzle,"SHOW DATABASES"))
  do_error(dbh,
            drizzle_errno(imp_dbh->pdrizzle),
            drizzle_error(imp_dbh->pdrizzle),
            drizzle_sqlstate(imp_dbh->pdrizzle));
  res= drizzle_store_result(imp_dbh->pdrizzle);
  if (!res  && (!drizzle_db_reconnect(dbh)))
  {
  do_error(dbh, drizzle_errno(imp_dbh->pdrizzle),
  drizzle_error(imp_dbh->pdrizzle), drizzle_sqlstate(imp_dbh->pdrizzle));
  }
  else
  {
  EXTEND(sp, drizzle_num_rows(res));
  while ((cur = drizzle_fetch_row(res)))
  {
    PUSHs(sv_2mortal((SV*)newSVpv(cur[0], strlen(cur[0]))));
  }
  drizzle_free_result(res);
}


void
do(dbh, statement, attr=Nullsv, ...)
  SV *        dbh
  SV *	statement
  SV *        attr
  PROTOTYPE: $$;$@
  CODE:
{
  D_imp_dbh(dbh);
  int num_params= 0;
  int retval;
  struct imp_sth_ph_st* params= NULL;
  DRIZZLE_RES* result= NULL;
  if (items > 3)
  {
    /*  Handle binding supplied values to placeholders	   */
    /*  Assume user has passed the correct number of parameters  */
    int i;
    num_params= items-3;
    Newz(0, params, sizeof(*params)*num_params, struct imp_sth_ph_st);
    for (i= 0;  i < num_params;  i++)
    {
      params[i].value= ST(i+3);
      params[i].type= SQL_VARCHAR;
    }
  }
  retval = drizzle_st_internal_execute(dbh, statement, attr, num_params,
                                       params, &result, imp_dbh->pdrizzle, 0);
  if (params)
    Safefree(params);

  if (result)
  {
    drizzle_free_result(result);
    result= 0;
  }
  /* remember that dbd_st_execute must return <= -2 for error	*/
  if (retval == 0)		/* ok with no rows affected	*/
    XST_mPV(0, "0E0");	/* (true but zero)		*/
  else if (retval < -1)	/* -1 == unknown number of rows	*/
    XST_mUNDEF(0);		/* <= -2 means error   		*/
  else
    XST_mIV(0, retval);	/* typically 1, rowcount or -1	*/
}


SV*
ping(dbh)
    SV* dbh;
  PROTOTYPE: $
  CODE:
    {
      int retval;
      D_imp_dbh(dbh);
      retval = (drizzle_ping(imp_dbh->pdrizzle) == 0);
      if (!retval) {
	if (drizzle_db_reconnect(dbh)) {
	  retval = (drizzle_ping(imp_dbh->pdrizzle) == 0);
	}
      }
      RETVAL = boolSV(retval);
    }
  OUTPUT:
    RETVAL



void
quote(dbh, str, type=NULL)
    SV* dbh
    SV* str
    SV* type
  PROTOTYPE: $$;$
  PPCODE:
    {
        SV* quoted = dbd_db_quote(dbh, str, type);
	ST(0) = quoted ? sv_2mortal(quoted) : str;
	XSRETURN(1);
    }


MODULE = DBD::drizzle    PACKAGE = DBD::drizzle::st

int
more_results(sth)
    SV *	sth
    CODE:
{
  D_imp_sth(sth);
  int retval;
  if (dbd_st_more_results(sth, imp_sth))
  {
    RETVAL=1;
  }
  else
  {
    RETVAL=0;
  }
}
    OUTPUT:
      RETVAL

int
dataseek(sth, pos)
    SV* sth
    int pos
  PROTOTYPE: $$
  CODE:
{
  D_imp_sth(sth);
  if (imp_sth->result) {
    drizzle_data_seek(imp_sth->result, pos);
    RETVAL = 1;
  } else {
    RETVAL = 0;
    do_error(sth, JW_ERR_NOT_ACTIVE, "Statement not active" ,NULL);
  }
}
  OUTPUT:
    RETVAL

void
rows(sth)
    SV* sth
  CODE:
    D_imp_sth(sth);
    char buf[64];
  /* fix to make rows able to handle errors and handle max value from 
     affected rows.
     if drizzle_affected_row returns an error, it's value is 18446744073709551614,
     while a (uint64_t)-1 is  18446744073709551615, so we have to add 1 to
     imp_sth->row_num to know if there's an error
  */
  if (imp_sth->row_num+1 ==  (uint64_t) -1)
    sprintf(buf, "%d", -1);
  else
    sprintf(buf, "%llu", imp_sth->row_num);

  ST(0) = sv_2mortal(newSVpvn(buf, strlen(buf)));



MODULE = DBD::drizzle    PACKAGE = DBD::drizzle::GetInfo

# This probably should be grabed out of some ODBC types header file
#define SQL_CATALOG_NAME_SEPARATOR 41
#define SQL_CATALOG_TERM 42
#define SQL_DBMS_VER 18
#define SQL_IDENTIFIER_QUOTE_CHAR 29
#define SQL_MAXIMUM_STATEMENT_LENGTH 105
#define SQL_MAXIMUM_TABLES_IN_SELECT 106
#define SQL_MAX_TABLE_NAME_LEN 35
#define SQL_SERVER_NAME 13


#  dbd_drizzle_getinfo()
#  Return ODBC get_info() information that must needs be accessed from C
#  This is an undocumented function that should only
#  be used by DBD::drizzle::GetInfo.

void
dbd_drizzle_get_info(dbh, sql_info_type)
    SV* dbh
    SV* sql_info_type
  CODE:
    D_imp_dbh(dbh);
    IV type = 0;
    SV* retsv=NULL;
    bool using_322=0;

    if (SvMAGICAL(sql_info_type))
        mg_get(sql_info_type);

    if (SvOK(sql_info_type))
    	type = SvIV(sql_info_type);
    else
    	croak("get_info called with an invalied parameter");
    
    switch(type) {
    	case SQL_CATALOG_NAME_SEPARATOR:
	    /* (dbc->flag & FLAG_NO_CATALOG) ? WTF is in flag ? */
	    retsv = newSVpv(".",1);
	    break;
	case SQL_CATALOG_TERM:
	    /* (dbc->flag & FLAG_NO_CATALOG) ? WTF is in flag ? */
	    retsv = newSVpv("database",8);
	    break;
	case SQL_DBMS_VER:
	    retsv = newSVpv(
	        imp_dbh->pdrizzle->server_version,
		strlen(imp_dbh->pdrizzle->server_version)
	    );
	    break;
	case SQL_IDENTIFIER_QUOTE_CHAR:
	    /*XXX What about a DB started in ANSI mode? */
	    /* Swiped from MyODBC's get_info.c */
	    retsv = newSVpv("`", 1);
	    break;
	case SQL_MAXIMUM_STATEMENT_LENGTH:
	    retsv = newSViv(8192);
	    break;
	case SQL_MAXIMUM_TABLES_IN_SELECT:
	    /* newSViv((sizeof(int) > 32) ? sizeof(int)-1 : 31 ); in general? */
	    retsv= newSViv((sizeof(int) == 64 ) ? 63 : 31 );
	    break;
	case SQL_MAX_TABLE_NAME_LEN:
	    retsv= newSViv(NAME_LEN);
	    break;
	case SQL_SERVER_NAME:
	    retsv= newSVpv(imp_dbh->pdrizzle->host_info,strlen(imp_dbh->pdrizzle->host_info));
	    break;
    	default:
 		croak("Unknown SQL Info type: %i",dbh);
    }
    ST(0) = sv_2mortal(retsv);

