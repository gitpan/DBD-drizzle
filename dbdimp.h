/*
 *  DBD::drizzle - DBI driver for the Drizzle database
 *
 *  Copyright (c) 2008       Patrick Galbraith
 *
 *  Based on DBD::Oracle; DBD::Oracle is
 *
 *  Copyright (c) 1994,1995  Tim Bunce
 *
 *  You may distribute this under the terms of either the GNU General Public
 *  License or the Artistic License, as specified in the Perl README file.
 *
 */

/*
 *  Header files we use
 */
#include <DBIXS.h>  /* installed by the DBI module                        */
#include <libdrizzle/drizzle.h>
#include <drizzled/common.h>  
#include <drizzled/error.h>  

#include <libdrizzle/errmsg.h> 

/*
 * This is the version of MySQL wherer
 * the server will be used to process prepare
 * statements as opposed to emulation in the driver
*/
#define SQL_STATE_VERSION 40101
#define WARNING_COUNT_VERSION 40101
#define FIELD_CHARSETNR_VERSION 40101 /* should equivalent to 4.1.0  */
#define MULTIPLE_RESULT_SET_VERSION 40102
#define SERVER_PREPARE_VERSION 40103
#define LIMIT_PLACEHOLDER_VERSION 50100
#define GEO_DATATYPE_VERSION 50007
#define NEW_DATATYPE_VERSION 50003
#define SSL_VERIFY_VERSION 50023
#define DRIZZLE_VERSION_5_0 50001
/* This is to avoid the ugly #ifdef mess in dbdimp.c */
#if DRIZZLE_VERSION_ID < SQL_STATE_VERSION
#define drizzle_sqlstate(con) (NULL)
#endif


#if DRIZZLE_VERSION_ID < WARNING_COUNT_VERSION
#define drizzle_warning_count(con) 0
#endif

/*
 *  The following are return codes passed in $h->err in case of
 *  errors by DBD::drizzle.
 */
enum errMsgs {
    JW_ERR_CONNECT = 1,
    JW_ERR_SELECT_DB,
    JW_ERR_STORE_RESULT,
    JW_ERR_NOT_ACTIVE,
    JW_ERR_QUERY,
    JW_ERR_FETCH_ROW,
    JW_ERR_LIST_DB,
    JW_ERR_CREATE_DB,
    JW_ERR_DROP_DB,
    JW_ERR_LIST_TABLES,
    JW_ERR_LIST_FIELDS,
    JW_ERR_LIST_FIELDS_INT,
    JW_ERR_LIST_SEL_FIELDS,
    JW_ERR_NO_RESULT,
    JW_ERR_NOT_IMPLEMENTED,
    JW_ERR_ILLEGAL_PARAM_NUM,
    JW_ERR_MEM,
    JW_ERR_LIST_INDEX,
    JW_ERR_SEQUENCE,
    AS_ERR_EMBEDDED,
    TX_ERR_AUTOCOMMIT,
    TX_ERR_COMMIT,
    TX_ERR_ROLLBACK
};


/*
 *  Internal constants, used for fetching array attributes
 */
enum av_attribs {
    AV_ATTRIB_NAME = 0,
    AV_ATTRIB_TABLE,
    AV_ATTRIB_TYPE,
    AV_ATTRIB_SQL_TYPE,
    AV_ATTRIB_IS_PRI_KEY,
    AV_ATTRIB_IS_NOT_NULL,
    AV_ATTRIB_NULLABLE,
    AV_ATTRIB_LENGTH,
    AV_ATTRIB_IS_NUM,
    AV_ATTRIB_TYPE_NAME,
    AV_ATTRIB_PRECISION,
    AV_ATTRIB_SCALE,
    AV_ATTRIB_MAX_LENGTH,
    AV_ATTRIB_IS_KEY,
    AV_ATTRIB_IS_BLOB,
    AV_ATTRIB_IS_AUTO_INCREMENT,
    AV_ATTRIB_LAST         /*  Dummy attribute, never used, for allocation  */
};                         /*  purposes only                                */


/*
 *  This is our part of the driver handle. We receive the handle as
 *  an "SV*", say "drh", and receive a pointer to the structure below
 *  by declaring
 *
 *    D_imp_drh(drh);
 *
 *  This declares a variable called "imp_drh" of type
 *  "struct imp_drh_st *".
 */
typedef struct imp_drh_embedded_st {
    int state;
    SV * args;
    SV * groups;
} imp_drh_embedded_t;

struct imp_drh_st {
    dbih_drc_t com;         /* MUST be first element in structure   */
#if defined(DBD_DRIZZLE_EMBEDDED)
    imp_drh_embedded_t embedded;     /* */
#endif
};


/*
 *  Likewise, this is our part of the database handle, as returned
 *  by DBI->connect. We receive the handle as an "SV*", say "dbh",
 *  and receive a pointer to the structure below by declaring
 *
 *    D_imp_dbh(dbh);
 *
 *  This declares a variable called "imp_dbh" of type
 *  "struct imp_dbh_st *".
 */
struct imp_dbh_st {
    dbih_dbc_t com;         /*  MUST be first element in structure   */

    DRIZZLE *pdrizzle;
    int has_transactions;   /*  boolean indicating support for
			     *  transactions, currently always
			     *  TRUE for MySQL and always FALSE
			     *  for mSQL.
			     */
    bool auto_reconnect;
    struct {
	    unsigned int auto_reconnects_ok;
	    unsigned int auto_reconnects_failed;
    } stats;
    unsigned short int  bind_type_guessing;
    int use_drizzle_use_result; /* TRUE if execute should use
                               * drizzle_use_result rather than
                               * drizzle_store_result
                               */
    int use_server_side_prepare;
    int has_autodetect_prepare;
    bool enable_utf8;
};


/*
 *  The bind_param method internally uses this structure for storing
 *  parameters.
 */
typedef struct imp_sth_ph_st {
    SV* value;
    int type;
} imp_sth_ph_t;

/*
 *  The bind_param method internally uses this structure for storing
 *  parameters.
 */
typedef struct imp_sth_phb_st {
    union
    {
      long lval;
      double dval;
    } numeric_val;
    unsigned long   length;
    char            is_null;
} imp_sth_phb_t;

/*
 *  The dbd_describe uses this structure for storing
 *  fields meta info.
 *  Added ddata, ldata, lldata for accomodate
 *  being able to use different data types
 *  12.02.20004 PMG
 */
typedef struct imp_sth_fbh_st {
    unsigned long  length;
    bool           is_null;
    char           *data;
    int            charsetnr;
    double         ddata;
    long           ldata;
} imp_sth_fbh_t;


typedef struct imp_sth_fbind_st {
   unsigned long   * length;
   char            * is_null;
} imp_sth_fbind_t;


/*
 *  Finally our part of the statement handle. We receive the handle as
 *  an "SV*", say "dbh", and receive a pointer to the structure below
 *  by declaring
 *
 *    D_imp_sth(sth);
 *
 *  This declares a variable called "imp_sth" of type
 *  "struct imp_sth_st *".
 */
struct imp_sth_st {
    dbih_stc_t com;       /* MUST be first element in structure     */

#if (DRIZZLE_VERSION_ID >= SERVER_PREPARE_VERSION)
    DRIZZLE_STMT       *stmt;
    DRIZZLE_BIND       *bind;
    DRIZZLE_BIND       *buffer;
    imp_sth_phb_t    *fbind;
    imp_sth_fbh_t    *fbh;
    int              has_been_bound;
    int use_server_side_prepare;  /* server side prepare statements? */
#endif

    DRIZZLE_RES* result;       /* result                                 */
    int currow;           /* number of current row                  */
    int fetch_done;       /* mark that fetch done                   */
    uint64_t row_num;         /* total number of rows                   */

    int   done_desc;      /* have we described this sth yet ?	    */
    long  long_buflen;    /* length for long/longraw (if >0)	    */
    bool  long_trunc_ok;  /* is truncating a long an error	    */
    uint64_t insertid; /* ID of auto insert                      */
    int   warning_count;  /* Number of warnings after execute()     */
    imp_sth_ph_t* params; /* Pointer to parameter array             */
    AV* av_attr[AV_ATTRIB_LAST];/*  For caching array attributes        */
    int   use_drizzle_use_result;  /*  TRUE if execute should use     */
                          /* drizzle_use_result rather than           */
                          /* drizzle_store_result */
};


/*
 *  And last, not least: The prototype definitions.
 *
 * These defines avoid name clashes for multiple statically linked DBD's	*/
#define dbd_init		drizzle_dr_init
#define dbd_db_login		drizzle_db_login
#define dbd_db_do		drizzle_db_do
#define dbd_db_commit		drizzle_db_commit
#define dbd_db_rollback		drizzle_db_rollback
#define dbd_db_disconnect	drizzle_db_disconnect
#define dbd_db_destroy		drizzle_db_destroy
#define dbd_db_STORE_attrib	drizzle_db_STORE_attrib
#define dbd_db_FETCH_attrib	drizzle_db_FETCH_attrib
#define dbd_st_prepare		drizzle_st_prepare
#define dbd_st_execute		drizzle_st_execute
#define dbd_st_fetch		drizzle_st_fetch
#define dbd_st_more_results     drizzle_st_next_results
#define dbd_st_finish		drizzle_st_finish
#define dbd_st_destroy		drizzle_st_destroy
#define dbd_st_blob_read	drizzle_st_blob_read
#define dbd_st_STORE_attrib	drizzle_st_STORE_attrib
#define dbd_st_FETCH_attrib	drizzle_st_FETCH_attrib
#define dbd_st_FETCH_internal	drizzle_st_FETCH_internal
#define dbd_describe		drizzle_describe
#define dbd_bind_ph		drizzle_bind_ph
#define BindParam		drizzle_st_bind_param
#define mymsql_constant         drizzle_constant
#define do_warn			drizzle_dr_warn
#define do_error		drizzle_dr_error
#define dbd_db_type_info_all    drizzle_db_type_info_all
#define dbd_db_quote            drizzle_db_quote

#ifdef DBD_DRIZZLE_INSERT_ID_IS_GOOD /* prototype was broken in some versions of dbi */
#define dbd_db_last_insert_id   drizzle_db_last_insert_id
#endif

#include <dbd_xsh.h>
void    do_error (SV* h, int rc, const char *what, const char *sqlstate);

SV	*dbd_db_fieldlist (DRIZZLE_RES* res);

void    dbd_preparse (imp_sth_t *imp_sth, SV *statement);
uint64_t drizzle_st_internal_execute(SV *,
                                       SV *,
                                       SV *,
                                       int,
                                       imp_sth_ph_t *,
                                       DRIZZLE_RES **,
                                       DRIZZLE *,
                                       int);

#if DRIZZLE_VERSION_ID >= SERVER_PREPARE_VERSION
uint64_t drizzle_st_internal_execute41(SV *,
                                         int,
                                         DRIZZLE_RES **,
                                         DRIZZLE_STMT *,
                                         DRIZZLE_BIND *,
                                         int *);


int drizzle_st_clean_cursor(SV*, imp_sth_t*);
#endif


AV* dbd_db_type_info_all (SV* dbh, imp_dbh_t* imp_dbh);
SV* dbd_db_quote(SV*, SV*, SV*);
extern DRIZZLE *drizzle_dr_connect(
                SV*, DRIZZLE*, char*, char*, char*, char*, char*,
			       char*, imp_dbh_t*);

extern int drizzle_db_reconnect(SV*);
int drizzle_st_free_result_sets (SV * sth, imp_sth_t * imp_sth);
static char *safe_hv_fetch(HV *hv, const char *name, int name_length);
