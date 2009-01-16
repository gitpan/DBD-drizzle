/*
 *  DBD::drizzle - DBI driver for the drizzle database
 *
 *  Copyright (c) 2008       Patrick Galbraith
 *
 *  You may distribute this under the terms of either the GNU General Public
 *  License or the Artistic License, as specified in the Perl README file.
 *
 */


#include "dbdimp.h"

#if defined(WIN32)  &&  defined(WORD)
#undef WORD
typedef short WORD;
#endif

DBISTATE_DECLARE;

typedef struct sql_type_info_s
{
    const char *type_name;
    int data_type;
    int column_size;
    const char *literal_prefix;
    const char *literal_suffix;
    const char *create_params;
    int nullable;
    int case_sensitive;
    int searchable;
    int unsigned_attribute;
    int fixed_prec_scale;
    int auto_unique_value;
    const char *local_type_name;
    int minimum_scale;
    int maximum_scale;
    int num_prec_radix;
    int sql_datatype;
    int sql_datetime_sub;
    int interval_precision;
    int native_type;
    int is_num;
} sql_type_info_t;


/*

  This function manually counts the number of placeholders in an SQL statement,
  used for emulated prepare statements

*/
static int
count_params(char *statement)
{
  char* ptr = statement;
  int num_params = 0;
  char c;
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, ">count_params statement %s\n", statement);

  while ( (c = *ptr++) )
  {
    switch (c) {
    case '`':
    case '"':
    case '\'':
      /* Skip string */
      {
        char end_token = c;
        while ((c = *ptr)  &&  c != end_token)
        {
          if (c == '\\')
            if (! *ptr)
              continue;

          ++ptr;
        }
        if (c)
          ++ptr;
        break;
      }

    case '?':
      ++num_params;
      break;

    default:
      break;
    }
  }
  return num_params;
}

/*
  allocate memory in statement handle per number of placeholders
*/
static imp_sth_ph_t *alloc_param(int num_params)
{
  imp_sth_ph_t *params;

  if (num_params)
    Newz(908, params, (unsigned int) num_params, imp_sth_ph_t);
  else
    params= NULL;

  return params;
}

/*
  free statement param structure per num_params
*/
static void
free_param(imp_sth_ph_t *params, int num_params)
{
  if (params)
  {
    int i;
    for (i= 0;  i < num_params;  i++)
    {
      imp_sth_ph_t *ph= params+i;
      if (ph->value)
      {
        (void) SvREFCNT_dec(ph->value);
        ph->value= NULL;
      }
    }
    Safefree(params);
  }
}

/* 
  Convert a Drizzle type to a type that perl can handle

  NOTE: In the future we may want to return a struct with a lot of
  information for each type
*/

static enum enum_field_types drizzle_to_perl_type(enum enum_field_types type)
{
  static enum enum_field_types enum_type;

  switch (type) {
  case DRIZZLE_TYPE_DOUBLE:
    enum_type= DRIZZLE_TYPE_DOUBLE;
    break;

  case DRIZZLE_TYPE_TINY:
  case DRIZZLE_TYPE_LONG:
    enum_type= DRIZZLE_TYPE_LONG;
    break;

  case DRIZZLE_TYPE_NEWDECIMAL:
    enum_type= DRIZZLE_TYPE_NEWDECIMAL;
    break;

  case DRIZZLE_TYPE_LONGLONG:			/* No longlong in perl */
  case DRIZZLE_TYPE_TIME:
  case DRIZZLE_TYPE_DATETIME:
  case DRIZZLE_TYPE_TIMESTAMP:
  case DRIZZLE_TYPE_VARCHAR:
    enum_type= DRIZZLE_TYPE_VARCHAR;
    break;

  case DRIZZLE_TYPE_BLOB:
    enum_type= DRIZZLE_TYPE_BLOB;
    break;

  default:
    enum_type= DRIZZLE_TYPE_VARCHAR;    /* MySQL can handle all types as strings */
  }
  return(enum_type);
}


/*
  constructs an SQL statement previously prepared with
  actual values replacing placeholders
*/
static char *parse_params(
                          DRIZZLE *con,
                          char *statement,
                          STRLEN *slen_ptr,
                          imp_sth_ph_t* params,
                          int num_params,
                          bool bind_type_guessing)
{

  char *salloc, *statement_ptr;
  char *statement_ptr_end, *ptr, *valbuf;
  char *cp, *end;
  int alen, i;
  int slen= *slen_ptr;
  int limit_flag= 0;
  STRLEN vallen;
  imp_sth_ph_t *ph;

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, ">parse_params statement %s\n", statement);

  if (num_params == 0)
    return NULL;

  while (isspace(*statement))
  {
    ++statement;
    --slen;
  }

  /* Calculate the number of bytes being allocated for the statement */
  alen= slen;

  for (i= 0, ph= params; i < num_params; i++, ph++)
  {
    int defined= 0;
    if (ph->value)
    {
      if (SvMAGICAL(ph->value))
        mg_get(ph->value);
      if (SvOK(ph->value))
        defined=1;
    }
    if (!defined)
      alen+= 3;  /* Erase '?', insert 'NULL' */
    else
    {
      valbuf= SvPV(ph->value, vallen);
      alen+= 2+vallen+1;
      /* this will most likely not happen since line 214 */
      /* of drizzle.xs hardcodes all types to SQL_VARCHAR */
      if (!ph->type)
      {
        if ( bind_type_guessing > 1 )
        {
          valbuf= SvPV(ph->value, vallen);
          ph->type= SQL_INTEGER;

          if (parse_number(valbuf, vallen, &end) != 0)
          {
              ph->type= SQL_VARCHAR;
          }
        }
        else if (bind_type_guessing)
          ph->type= SvNIOK(ph->value) ? SQL_INTEGER : SQL_VARCHAR;
        else
          ph->type= SQL_VARCHAR;
      }
    }
  }

  /* Allocate memory, why *2, well, because we have ptr and statement_ptr */
  New(908, salloc, alen*2, char);
  ptr= salloc;

  i= 0;
 /* Now create the statement string; compare count_params above */
  statement_ptr_end= (statement_ptr= statement)+ slen;

  while (statement_ptr < statement_ptr_end)
  {
    /* LIMIT should be the last part of the query, in most cases */
    if (! limit_flag)
    {
      /*
        it would be good to be able to handle any number of cases and orders
      */
      if ((*statement_ptr == 'l' || *statement_ptr == 'L') &&
          (!strncmp(statement_ptr+1, "imit ?", 6) ||
           !strncmp(statement_ptr+1, "IMIT ?", 6)))
      {
        limit_flag = 1;
      }
    }
    switch (*statement_ptr)
    {
      case '`':
      case '\'':
      case '"':
      /* Skip string */
      {
        char endToken = *statement_ptr++;
        *ptr++ = endToken;
        while (statement_ptr != statement_ptr_end &&
               *statement_ptr != endToken)
        {
          if (*statement_ptr == '\\')
          {
            *ptr++ = *statement_ptr++;
            if (statement_ptr == statement_ptr_end)
	      break;
	  }
          *ptr++= *statement_ptr++;
	}
	if (statement_ptr != statement_ptr_end)
          *ptr++= *statement_ptr++;
      }
      break;

      case '?':
        /* Insert parameter */
        statement_ptr++;
        if (i >= num_params)
        {
          break;
        }

        ph = params+ (i++);
        if (!ph->value  ||  !SvOK(ph->value))
        {
          *ptr++ = 'N';
          *ptr++ = 'U';
          *ptr++ = 'L';
          *ptr++ = 'L';
        }
        else
        {
          int is_num = FALSE;

          valbuf= SvPV(ph->value, vallen);
          if (valbuf)
          {
            switch (ph->type)
            {
              case SQL_NUMERIC:
              case SQL_DECIMAL:
              case SQL_INTEGER:
              case SQL_SMALLINT:
              case SQL_FLOAT:
              case SQL_REAL:
              case SQL_DOUBLE:
              case SQL_BIGINT:
              case SQL_TINYINT:
                is_num = TRUE;
                break;
            }

            /* we're at the end of the query, so any placeholders if */
            /* after a LIMIT clause will be numbers and should not be quoted */
            if (limit_flag == 1)
              is_num = TRUE;

            if (!is_num)
            {
              *ptr++ = '\'';
              ptr += drizzle_escape_string(ptr, valbuf, vallen);
              *ptr++ = '\'';
            }
            else
            {
              parse_number(valbuf, vallen, &end);
              for (cp= valbuf; cp < end; cp++)
                  *ptr++= *cp;
            }
          }
        }
        break;

	/* in case this is a nested LIMIT */
      case ')':
        limit_flag = 0;
	*ptr++ = *statement_ptr++;
        break;

      default:
        *ptr++ = *statement_ptr++;
        break;

    }
  }

  *slen_ptr = ptr - salloc;
  *ptr++ = '\0';

  return(salloc);
}

int bind_param(imp_sth_ph_t *ph, SV *value, IV sql_type)
{
  if (ph->value)
  {
    if (SvMAGICAL(ph->value))
      mg_get(ph->value);
    (void) SvREFCNT_dec(ph->value);
  }

  ph->value= newSVsv(value);

  if (sql_type)
    ph->type = sql_type;

  return TRUE;
}

static const sql_type_info_t SQL_GET_TYPE_INFO_values[]= {
  /* 0 */
  { "varchar",    SQL_VARCHAR,                    255, "'",  "'",  "max length",
    1, 0, 3, 0, 0, 0, "variable length string",
    0, 0, 0,
    SQL_VARCHAR, 0, 0,
    DRIZZLE_TYPE_VARCHAR,  0,
  },
  /* 1 */
  { "decimal",   SQL_DECIMAL,                      15, NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "double",
    0, 6, 2,
    SQL_DECIMAL, 0, 0,
    DRIZZLE_TYPE_NEWDECIMAL,     1
  },
  /* 2 */
  { "tinyint",   SQL_TINYINT,                       3, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Tiny integer",
    0, 0, 10,
    SQL_TINYINT, 0, 0,
    DRIZZLE_TYPE_TINY,     1
  },
  /* 3 */
  { "smallint",  SQL_SMALLINT,                      5, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Short integer",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
    DRIZZLE_TYPE_LONG,     1
  },
  /* 4 */
  { "integer",   SQL_INTEGER,                      5, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    DRIZZLE_TYPE_LONG,     1
  },
  /* 5 */
  { "float",     SQL_REAL,                          7,  NULL, NULL, NULL,
    1, 0, 0, 0, 0, 0, "float",
    0, 2, 10,
    SQL_FLOAT, 0, 0,
    DRIZZLE_TYPE_DOUBLE,     1
  },
  /* 6 */
  { "double",    SQL_FLOAT,                       15,  NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "double",
    0, 4, 2,
    SQL_FLOAT, 0, 0,
    DRIZZLE_TYPE_DOUBLE,     1
  },
  /* 7 */
  { "double",    SQL_DOUBLE,                       15,  NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "double",
    0, 4, 10,
    SQL_DOUBLE, 0, 0,
    DRIZZLE_TYPE_DOUBLE,     1
  },
  /*
    DRIZZLE_TYPE_NULL ?
  */
  /* 8 */
  { "timestamp", SQL_TIMESTAMP,                    14, "'", "'", NULL,
    0, 0, 3, 0, 0, 0, "timestamp",
    0, 0, 0,
    SQL_TIMESTAMP, 0, 0,
    DRIZZLE_TYPE_TIMESTAMP,     0
  },
  /* 9 */
  { "bigint",    SQL_BIGINT,                       19, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Longlong integer",
    0, 0, 10,
    SQL_BIGINT, 0, 0,
    DRIZZLE_TYPE_LONGLONG,     1
  },
  /* 10 */
  { "mediumint", SQL_INTEGER,                       8, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Medium integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    DRIZZLE_TYPE_LONG,     1
  },
  /* 11 */
  { "time", SQL_TIME, 6, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "time",
    0, 0, 0,
    SQL_TIME, 0, 0,
    DRIZZLE_TYPE_TIME,     0
  },
  /* 12 */
  { "datetime",  SQL_TIMESTAMP, 21, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "datetime",
    0, 0, 0,
    SQL_TIMESTAMP, 0, 0,
    DRIZZLE_TYPE_DATETIME,     0
  },
  /* 13 */
  /*{ "date", SQL_DATE, 10, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "date",
    0, 0, 0,
    SQL_DATE, 0, 0,
    DRIZZLE_TYPE_NEWDATE,     0
  },
  */
  /* 14 */
  { "enum",      SQL_VARCHAR,                     255, "'",  "'",  NULL,
    1, 0, 1, 0, 0, 0, "enum(value1,value2,value3...)",
    0, 0, 0,
    0, 0, 0,
    DRIZZLE_TYPE_ENUM,     0
  },
  /* 15 */
  { "blob",       SQL_LONGVARBINARY,              65535, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object (0-65535)",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
    DRIZZLE_TYPE_BLOB,     0
  },
  { "tinyblob",  SQL_VARBINARY,                 255, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object (0-255) ",
    0, 0, 0,
    SQL_VARBINARY, 0, 0,
    DRIZZLE_TYPE_BLOB,        0
  },
  { "mediumblob", SQL_LONGVARBINARY,           16777215, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
    DRIZZLE_TYPE_BLOB, 0
  },
  { "longblob",   SQL_LONGVARBINARY,         2147483647, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object, use mediumblob instead",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
    DRIZZLE_TYPE_BLOB,   0
  },
  { "char",       SQL_CHAR,                       255, "'",  "'",  "max length",
    1, 0, 3, 0, 0, 0, "string",
    0, 0, 0,
    SQL_CHAR, 0, 0,
    DRIZZLE_TYPE_VARCHAR,   0
  },

  { "decimal",            SQL_NUMERIC,            15,  NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "double",
    0, 6, 2,
    SQL_NUMERIC, 0, 0,
    DRIZZLE_TYPE_NEWDECIMAL,   1
  },
  { "tinyint unsigned",   SQL_TINYINT,              3, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Tiny integer unsigned",
    0, 0, 10,
    SQL_TINYINT, 0, 0,
    DRIZZLE_TYPE_TINY,        1
  },
  { "smallint unsigned",  SQL_SMALLINT,             5, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Short integer unsigned",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
    DRIZZLE_TYPE_LONG,       1
  },
  { "mediumint unsigned", SQL_INTEGER,              8, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Medium integer unsigned",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    DRIZZLE_TYPE_LONG,       1
  },
  { "int unsigned",       SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "integer unsigned",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    DRIZZLE_TYPE_LONG,        1
  },
  { "int",                SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    DRIZZLE_TYPE_LONG,        1
  },
  { "integer unsigned",   SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    DRIZZLE_TYPE_LONG,        1
  },
  { "bigint unsigned",    SQL_BIGINT,              20, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Longlong integer unsigned",
    0, 0, 10,
    SQL_BIGINT, 0, 0,
    DRIZZLE_TYPE_LONGLONG,    1
  },
  { "text",               SQL_LONGVARCHAR,      65535, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "large text object (0-65535)",
    0, 0, 0,
    SQL_LONGVARCHAR, 0, 0,
    DRIZZLE_TYPE_BLOB,        0
  },
  { "mediumtext",         SQL_LONGVARCHAR,   16777215, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "large text object",
    0, 0, 0,
    SQL_LONGVARCHAR, 0, 0,
    DRIZZLE_TYPE_BLOB, 0
  },
  { "mediumint unsigned auto_increment", SQL_INTEGER, 8, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "Medium integer unsigned auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, DRIZZLE_TYPE_LONG, 1,
  },
  { "tinyint unsigned auto_increment", SQL_TINYINT, 3, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "tinyint unsigned auto_increment", 0, 0, 10,
    SQL_TINYINT, 0, 0, DRIZZLE_TYPE_TINY, 1
  },

  { "smallint auto_increment", SQL_SMALLINT, 5, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "smallint auto_increment", 0, 0, 10,
    SQL_SMALLINT, 0, 0, DRIZZLE_TYPE_LONG, 1
  },

  { "int unsigned auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "integer unsigned auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, DRIZZLE_TYPE_LONG, 1
  },

  { "mediumint", SQL_INTEGER, 7, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Medium integer", 0, 0, 10,
    SQL_INTEGER, 0, 0, DRIZZLE_TYPE_LONG, 1
  },

  { "bit", SQL_BIT, 1, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "char(1)", 0, 0, 0,
    SQL_BIT, 0, 0, DRIZZLE_TYPE_LONG, 0
  },

  { "numeric", SQL_NUMERIC, 19, NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "numeric", 0, 19, 10,
    SQL_NUMERIC, 0, 0, DRIZZLE_TYPE_NEWDECIMAL, 1,
  },

  { "integer unsigned auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "integer unsigned auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, DRIZZLE_TYPE_LONG, 1,
  },

  { "mediumint unsigned", SQL_INTEGER, 8, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Medium integer unsigned", 0, 0, 10,
    SQL_INTEGER, 0, 0, DRIZZLE_TYPE_LONG, 1
  },

  { "smallint unsigned auto_increment", SQL_SMALLINT, 5, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "smallint unsigned auto_increment", 0, 0, 10,
    SQL_SMALLINT, 0, 0, DRIZZLE_TYPE_LONG, 1
  },

  { "int auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "integer auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, DRIZZLE_TYPE_LONG, 1
  },

  { "long varbinary", SQL_LONGVARBINARY, 16777215, "0x", NULL, NULL,
    1, 0, 3, 0, 0, 0, "mediumblob", 0, 0, 0,
    SQL_LONGVARBINARY, 0, 0, DRIZZLE_TYPE_BLOB, 0
  },

  { "double auto_increment", SQL_FLOAT, 15, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "double auto_increment", 0, 4, 2,
    SQL_FLOAT, 0, 0, DRIZZLE_TYPE_DOUBLE, 1
  },

  { "double auto_increment", SQL_DOUBLE, 15, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "double auto_increment", 0, 4, 10,
    SQL_DOUBLE, 0, 0, DRIZZLE_TYPE_DOUBLE, 1
  },

  { "integer auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "integer auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, DRIZZLE_TYPE_LONG, 1,
  },

  { "bigint auto_increment", SQL_BIGINT, 19, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "bigint auto_increment", 0, 0, 10,
    SQL_BIGINT, 0, 0, DRIZZLE_TYPE_LONGLONG, 1
  },

  { "bit auto_increment", SQL_BIT, 1, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "char(1) auto_increment", 0, 0, 0,
    SQL_BIT, 0, 0, DRIZZLE_TYPE_TINY, 1
  },

  { "mediumint auto_increment", SQL_INTEGER, 7, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "Medium integer auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, DRIZZLE_TYPE_LONG, 1
  },

  { "float auto_increment", SQL_REAL, 7, NULL, NULL, NULL,
    0, 0, 0, 0, 0, 1, "float auto_increment", 0, 2, 10,
    SQL_FLOAT, 0, 0, DRIZZLE_TYPE_DOUBLE, 1
  },

  { "long varchar", SQL_LONGVARCHAR, 16777215, "'", "'", NULL,
    1, 0, 3, 0, 0, 0, "mediumtext", 0, 0, 0,
    SQL_LONGVARCHAR, 0, 0, DRIZZLE_TYPE_BLOB, 1

  },

  { "tinyint auto_increment", SQL_TINYINT, 3, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "tinyint auto_increment", 0, 0, 10,
    SQL_TINYINT, 0, 0, DRIZZLE_TYPE_TINY, 1
  },

  { "bigint unsigned auto_increment", SQL_BIGINT, 20, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "bigint unsigned auto_increment", 0, 0, 10,
    SQL_BIGINT, 0, 0, DRIZZLE_TYPE_LONGLONG, 1
  },

/* END MORE STUFF */
};

/*
  static const sql_type_info_t* native2sql (int t)
*/
static const sql_type_info_t *native2sql(int t)
{
  switch (t) {
    case DRIZZLE_TYPE_VARCHAR:  return &SQL_GET_TYPE_INFO_values[0];
    case DRIZZLE_TYPE_NEWDECIMAL:  return &SQL_GET_TYPE_INFO_values[1];
    case DRIZZLE_TYPE_TINY:        return &SQL_GET_TYPE_INFO_values[2];
    case DRIZZLE_TYPE_LONG:        return &SQL_GET_TYPE_INFO_values[4];
    case DRIZZLE_TYPE_DOUBLE:      return &SQL_GET_TYPE_INFO_values[7];
    case DRIZZLE_TYPE_TIMESTAMP:   return &SQL_GET_TYPE_INFO_values[8];
    case DRIZZLE_TYPE_LONGLONG:    return &SQL_GET_TYPE_INFO_values[9];
    case DRIZZLE_TYPE_TIME:        return &SQL_GET_TYPE_INFO_values[11];
    case DRIZZLE_TYPE_DATETIME:    return &SQL_GET_TYPE_INFO_values[12];
                                   /*case DRIZZLE_TYPE_NEWDATE:     return &SQL_GET_TYPE_INFO_values[13];*/
    case DRIZZLE_TYPE_ENUM:        return &SQL_GET_TYPE_INFO_values[14];
    case DRIZZLE_TYPE_BLOB:        return &SQL_GET_TYPE_INFO_values[15];
    default:                     return &SQL_GET_TYPE_INFO_values[0];
  }
}


#define SQL_GET_TYPE_INFO_num \
	(sizeof(SQL_GET_TYPE_INFO_values)/sizeof(sql_type_info_t))


/***************************************************************************
 *
 *  Name:    dbd_init
 *
 *  Purpose: Called when the driver is installed by DBI
 *
 *  Input:   dbistate - pointer to the DBIS variable, used for some
 *               DBI internal things
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void dbd_init(dbistate_t* dbistate)
{
    DBIS = dbistate;
}


/**************************************************************************
 *
 *  Name:    do_error, do_warn
 *
 *  Purpose: Called to associate an error code and an error message
 *           to some handle
 *
 *  Input:   h - the handle in error condition
 *           rc - the error code
 *           what - the error message
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void do_error(SV* h, int rc, const char* what, const char* sqlstate)
{
  D_imp_xxh(h);
  STRLEN lna;
  SV *errstr;
  SV *errstate;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "\t\t--> do_error\n");
  errstr= DBIc_ERRSTR(imp_xxh);
  sv_setiv(DBIc_ERR(imp_xxh), (IV)rc);	/* set err early	*/
  sv_setpv(errstr, what);

  if (sqlstate)
  {
    errstate= DBIc_STATE(imp_xxh);
    sv_setpvn(errstate, sqlstate, 5);
  }

  /* NO EFFECT DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), errstr); */
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "%s error %d recorded: %s\n",
    what, rc, SvPV(errstr,lna));
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "\t\t<-- do_error\n");
}

/*
  void do_warn(SV* h, int rc, char* what)
*/
void do_warn(SV* h, int rc, char* what)
{
  D_imp_xxh(h);
  STRLEN lna;

  SV *errstr = DBIc_ERRSTR(imp_xxh);
  sv_setiv(DBIc_ERR(imp_xxh), (IV)rc);	/* set err early	*/
  sv_setpv(errstr, what);
  /* NO EFFECT DBIh_EVENT2(h, WARN_event, DBIc_ERR(imp_xxh), errstr);*/
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "%s warning %d recorded: %s\n",
    what, rc, SvPV(errstr,lna));
  warn("%s", what);
}

#define DBD_DRIZZLE_NAMESPACE "DBD::drizzle::QUIET";

#define doquietwarn(s) \
  { \
    SV* sv = perl_get_sv(DBD_DRIZZLE_NAMESPACE, FALSE);  \
    if (!sv  ||  !SvTRUE(sv)) { \
      warn s; \
    } \
  }


/***************************************************************************
 *
 *  Name:    drizzle_dr_connect
 *
 *  Purpose: Replacement for drizzle_connect
 *
 *  Input:   DRIZZLE *con - Pointer to a DRIZZLE structure being
 *             initialized
 *           char* drizzle_socket - Name of a UNIX socket being used
 *             or NULL
 *           char* host - Host name being used or NULL for localhost
 *           char* port - Port number being used or NULL for default
 *           char* user - User name being used or NULL
 *           char* password - Password being used or NULL
 *           char* dbname - Database name being used or NULL
 *           char* imp_dbh - Pointer to internal dbh structure
 *
 *  Returns: The sock argument for success, NULL otherwise;
 *           you have to call do_error in the latter case.
 *
 **************************************************************************/

DRIZZLE *drizzle_dr_connect(
                            SV* dbh,
                            DRIZZLE *con,
                            char* drizzle_socket,
                            char* host,
                            char* port,
                            char* user,
                            char* password,
                            char* dbname,
                            imp_dbh_t *imp_dbh)
{
  int portNr;
  unsigned int client_flag;
  DRIZZLE *result;
  D_imp_xxh(dbh);

  portNr= (port && *port) ? atoi(port) : 0;

#ifdef DRIZZLE_NO_CLIENT_FOUND_ROWS
  client_flag = 0;
#else
  client_flag = CLIENT_FOUND_ROWS;
#endif
  drizzle_create(con);

  if (imp_dbh)
  {
    SV* sv = DBIc_IMP_DATA(imp_dbh);

    DBIc_set(imp_dbh, DBIcf_AutoCommit, &sv_yes);
    if (sv  &&  SvROK(sv))
    {
      HV* hv = (HV*) SvRV(sv);
      SV** svp;
      STRLEN lna;

      if ((svp = hv_fetch(hv, "drizzle_compression", 17, FALSE))  &&
          *svp && SvTRUE(*svp))
      {
        drizzle_options(con, DRIZZLE_OPT_COMPRESS, NULL);
      }
      if ((svp = hv_fetch(hv, "drizzle_connect_timeout", 21, FALSE))
          &&  *svp  &&  SvTRUE(*svp))
      {
        int to = SvIV(*svp);
        drizzle_options(con, DRIZZLE_OPT_CONNECT_TIMEOUT,
                        (const char *)&to);
      }
      if ((svp = hv_fetch(hv, "drizzle_read_default_file", 23, FALSE)) &&
          *svp  &&  SvTRUE(*svp))
      {
        char* df = SvPV(*svp, lna);
        drizzle_options(con, DRIZZLE_READ_DEFAULT_FILE, df);
      }
      if ((svp = hv_fetch(hv, "drizzle_read_default_group", 24,
                          FALSE))  &&
          *svp  &&  SvTRUE(*svp)) {
        char* gr = SvPV(*svp, lna);

        drizzle_options(con, DRIZZLE_READ_DEFAULT_GROUP, gr);
      }
      if ((svp = hv_fetch(hv,
                          "drizzle_client_found_rows", 23, FALSE)) && *svp)
      {
        if (SvTRUE(*svp))
          client_flag |= CLIENT_FOUND_ROWS;
        else
          client_flag &= ~CLIENT_FOUND_ROWS;
      }
      if ((svp = hv_fetch(hv, "drizzle_use_result", 16, FALSE)) && *svp)
        imp_dbh->use_drizzle_use_result = SvTRUE(*svp);

#if defined(CLIENT_MULTI_STATEMENTS)
      if ((svp = hv_fetch(hv, "drizzle_multi_statements", 22, FALSE)) && *svp)
      {
        if (SvTRUE(*svp))
          client_flag |= CLIENT_MULTI_STATEMENTS;
        else
          client_flag &= ~CLIENT_MULTI_STATEMENTS;
      }
#endif

      /* HELMUT */
#if defined(sv_utf8_decode)
      if ((svp = hv_fetch(hv, "drizzle_enable_utf8", 17, FALSE)) && *svp)
      {
      }
#endif

#if defined(DBD_DRIZZLE_WITH_SSL) && (defined(CLIENT_SSL))
      if ((svp = hv_fetch(hv, "drizzle_ssl", 9, FALSE))  &&  *svp)
      {
        if (SvTRUE(*svp))
        {
          char *client_key = NULL;
          char *client_cert = NULL;
          char *ca_file = NULL;
          char *ca_path = NULL;
          char *cipher = NULL;
          STRLEN lna;
          /*
            New code to utilise MySQLs new feature that verifies that the
            server's hostname that the client connects to matches that of
            the certificate
          */
          my_bool ssl_verify_true = 0;
          if ((svp = hv_fetch(hv, "drizzle_ssl_verify_server_cert", 
                              28, FALSE)) &&  *svp)
            ssl_verify_true = SvTRUE(*svp);

          if ((svp = hv_fetch(hv, "drizzle_ssl_client_key", 20, FALSE)) && *svp)
            client_key = SvPV(*svp, lna);

          if ((svp = hv_fetch(hv, "drizzle_ssl_client_cert", 21, FALSE)) &&
              *svp)
            client_cert = SvPV(*svp, lna);

          if ((svp = hv_fetch(hv, "drizzle_ssl_ca_file", 17, FALSE)) &&
              *svp)
            ca_file = SvPV(*svp, lna);

          if ((svp = hv_fetch(hv, "drizzle_ssl_ca_path", 17, FALSE)) &&
              *svp)
            ca_path = SvPV(*svp, lna);

          if ((svp = hv_fetch(hv, "drizzle_ssl_cipher", 16, FALSE)) &&
              *svp)
            cipher = SvPV(*svp, lna);

          drizzle_ssl_set(con, client_key, client_cert, ca_file,
                          ca_path, cipher);

          drizzle_options(con,
                          DRIZZLE_OPT_SSL_VERIFY_SERVER_CERT,
                          &ssl_verify_true);

          client_flag |= CLIENT_SSL;
        }
      }
#endif

      if ((svp = hv_fetch( hv, "drizzle_local_infile", 18, FALSE))  &&  *svp)
      {
        unsigned int flag = SvTRUE(*svp);
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBILOGFP,
                        "imp_dbh->drizzle_dr_connect: Using" \
                        " local infile %u.\n", flag);
        drizzle_options(con, DRIZZLE_OPT_LOCAL_INFILE, (const char *) &flag);
      }
    }
  }

  client_flag|= CLIENT_MULTI_RESULTS;

  result = drizzle_connect(con, host, user, password, dbname,
                                portNr, drizzle_socket, client_flag);

  if (result)
  {
    /*
      we turn off Mysql's auto reconnect and handle re-connecting ourselves
      so that we can keep track of when this happens.
    */
    result->reconnect=0;
  }
  return result;
}

/*
  safe_hv_fetch
*/
char *safe_hv_fetch(HV *hv, const char *name, int name_length)
{
  SV** svp;
  STRLEN len;
  char *res= NULL;

  if ((svp= hv_fetch(hv, name, name_length, FALSE)))
  {
    res= SvPV(*svp, len);
    if (!len)
      res= NULL;
  }
  return res;
}

/*
 Frontend for drizzle_dr_connect
*/
int my_login(SV* dbh, imp_dbh_t *imp_dbh)
{
  SV* sv;
  HV* hv;
  char* dbname;
  char* host;
  char* port;
  char* user;
  char* password;
  char* drizzle_socket;
  D_imp_xxh(dbh);

#define TAKE_IMP_DATA_VERSION 1
#if TAKE_IMP_DATA_VERSION
  if (DBIc_has(imp_dbh, DBIcf_IMPSET))
  { /* eg from take_imp_data() */
    if (DBIc_has(imp_dbh, DBIcf_ACTIVE))
    {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBILOGFP, "my_login skip connect\n");
      /* tell our parent we've adopted an active child */
      ++DBIc_ACTIVE_KIDS(DBIc_PARENT_COM(imp_dbh));
      return TRUE;
    }
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBILOGFP,
                    "my_login IMPSET but not ACTIVE so connect not skipped\n");
  }
#endif

  sv = DBIc_IMP_DATA(imp_dbh);

  if (!sv  ||  !SvROK(sv))
    return FALSE;

  hv = (HV*) SvRV(sv);
  if (SvTYPE(hv) != SVt_PVHV)
    return FALSE;

  host=		safe_hv_fetch(hv, "host", 4);
  port=		safe_hv_fetch(hv, "port", 4);
  user=		safe_hv_fetch(hv, "user", 4);
  password=	safe_hv_fetch(hv, "password", 8);
  dbname=	safe_hv_fetch(hv, "database", 8);
  drizzle_socket=	safe_hv_fetch(hv, "drizzle_socket", 12);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP,
		  "imp_dbh->my_login : dbname = %s, uid = %s, pwd = %s," \
		  "host = %s, port = %s\n",
		  dbname ? dbname : "NULL",
		  user ? user : "NULL",
		  password ? password : "NULL",
		  host ? host : "NULL",
		  port ? port : "NULL");

  if (!imp_dbh->pdrizzle) {
     Newz(908, imp_dbh->pdrizzle, 1, DRIZZLE);
  }
  return drizzle_dr_connect(
                                         dbh,
                                         imp_dbh->pdrizzle,
                                         drizzle_socket,
                                         host,
                                         port,
                                         user,
                                         password,
                                         dbname,
                                         imp_dbh) ?
    TRUE : FALSE;
}


/**************************************************************************
 *
 *  Name:    dbd_db_login
 *
 *  Purpose: Called for connecting to a database and logging in.
 *
 *  Input:   dbh - database handle being initialized
 *           imp_dbh - drivers private database handle data
 *           dbname - the database we want to log into; may be like
 *               "dbname:host" or "dbname:host:port"
 *           user - user name to connect as
 *           password - passwort to connect with
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
 *
 **************************************************************************/

int dbd_db_login(SV* dbh, imp_dbh_t* imp_dbh, char* dbname, char* user,
                 char* password) {
#ifdef dTHR
  dTHR;
#endif
  D_imp_xxh(dbh);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP,
                  "imp_dbh->connect: dsn = %s, uid = %s, pwd = %s\n",
                  dbname ? dbname : "NULL",
                  user ? user : "NULL",
                  password ? password : "NULL");

  imp_dbh->stats.auto_reconnects_ok= 0;
  imp_dbh->stats.auto_reconnects_failed= 0;
  imp_dbh->bind_type_guessing= FALSE;
  imp_dbh->has_transactions= TRUE;
  /* Safer we flip this to TRUE perl side if we detect a mod_perl env. */
  imp_dbh->auto_reconnect = FALSE;

  /* HELMUT */
#if defined(sv_utf8_decode)
  imp_dbh->enable_utf8 = FALSE;  /* initialize drizzle_enable_utf8 */
#endif

  if (!my_login(dbh, imp_dbh))
  {
    do_error(dbh, drizzle_errno(imp_dbh->pdrizzle),
             drizzle_error(imp_dbh->pdrizzle) ,drizzle_sqlstate(imp_dbh->pdrizzle));
    return FALSE;
  }

  /*
   *  Tell DBI, that dbh->disconnect should be called for this handle
 */
  DBIc_ACTIVE_on(imp_dbh);

  /* Tell DBI, that dbh->destroy should be called for this handle */
  DBIc_on(imp_dbh, DBIcf_IMPSET);

  return TRUE;
}


/***************************************************************************
 *
 *  Name:    dbd_db_commit
 *           dbd_db_rollback
 *
 *  Purpose: You guess what they should do. 
 *
 *  Input:   dbh - database handle being commited or rolled back
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
 *
 **************************************************************************/

int
dbd_db_commit(SV* dbh, imp_dbh_t* imp_dbh)
{
  if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
    return FALSE;

  if (imp_dbh->has_transactions)
  {
    if (drizzle_commit(imp_dbh->pdrizzle))
    {
      do_error(dbh, drizzle_errno(imp_dbh->pdrizzle), drizzle_error(imp_dbh->pdrizzle)
               ,drizzle_sqlstate(imp_dbh->pdrizzle));
      return FALSE;
    }
  }
  else
    do_warn(dbh, JW_ERR_NOT_IMPLEMENTED,
            "Commit ineffective because transactions are not available");
  return TRUE;
}

/*
 dbd_db_rollback
*/
int dbd_db_rollback(SV* dbh, imp_dbh_t* imp_dbh) {
  /* croak, if not in AutoCommit mode */
  if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
    return FALSE;

  if (imp_dbh->has_transactions)
  {
    if (drizzle_rollback(imp_dbh->pdrizzle))
    {
      do_error(dbh, drizzle_errno(imp_dbh->pdrizzle),
               drizzle_error(imp_dbh->pdrizzle),
               drizzle_sqlstate(imp_dbh->pdrizzle));
      return FALSE;
    }
  }
  else
    do_error(dbh, JW_ERR_NOT_IMPLEMENTED,
             "Rollback ineffective because transactions are not available" ,NULL);
  return TRUE;
}

/*
 ***************************************************************************
 *
 *  Name:    dbd_db_disconnect
 *
 *  Purpose: Disconnect a database handle from its database
 *
 *  Input:   dbh - database handle being disconnected
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
 *
 **************************************************************************/

int dbd_db_disconnect(SV* dbh, imp_dbh_t* imp_dbh)
{
#ifdef dTHR
    dTHR;
#endif
    D_imp_xxh(dbh);

    /* We assume that disconnect will always work       */
    /* since most errors imply already disconnected.    */
    DBIc_ACTIVE_off(imp_dbh);
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBILOGFP, "imp_dbh->pdrizzle: %lx\n",
		      (long) imp_dbh->pdrizzle);
    drizzle_close(imp_dbh->pdrizzle );

    /* We don't free imp_dbh since a reference still exists    */
    /* The DESTROY method is the only one to 'free' memory.    */
    return TRUE;
}


/***************************************************************************
 *
 *  Name:    dbd_discon_all
 *
 *  Purpose: Disconnect all database handles at shutdown time
 *
 *  Input:   dbh - database handle being disconnected
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
 *
 **************************************************************************/

int dbd_discon_all (SV *drh, imp_drh_t *imp_drh) {
#if defined(dTHR)
    dTHR;
#endif
  D_imp_xxh(drh);

  /* The disconnect_all concept is flawed and needs more work */
  if (!dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
    sv_setiv(DBIc_ERR(imp_drh), (IV)1);
    sv_setpv(DBIc_ERRSTR(imp_drh),
             (char*)"disconnect_all not implemented");
    /* NO EFFECT DBIh_EVENT2(drh, ERROR_event,
      DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh)); */
    return FALSE;
  }
  perl_destruct_level = 0;
  return FALSE;
}


/****************************************************************************
 *
 *  Name:    dbd_db_destroy
 *
 *  Purpose: Our part of the dbh destructor
 *
 *  Input:   dbh - database handle being destroyed
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void dbd_db_destroy(SV* dbh, imp_dbh_t* imp_dbh) {

    /*
     *  Being on the safe side never hurts ...
     */
  if (DBIc_ACTIVE(imp_dbh))
  {
    if (imp_dbh->has_transactions)
    {
      if (!DBIc_has(imp_dbh, DBIcf_AutoCommit))
        if (drizzle_rollback(imp_dbh->pdrizzle))
            do_error(dbh, TX_ERR_ROLLBACK,"ROLLBACK failed" ,NULL);
    }
    dbd_db_disconnect(dbh, imp_dbh);
  }
  Safefree(imp_dbh->pdrizzle);

  /* Tell DBI, that dbh->destroy must no longer be called */
  DBIc_off(imp_dbh, DBIcf_IMPSET);
}

/* 
 ***************************************************************************
 *
 *  Name:    dbd_db_STORE_attrib
 *
 *  Purpose: Function for storing dbh attributes; we currently support
 *           just nothing. :-)
 *
 *  Input:   dbh - database handle being modified
 *           imp_dbh - drivers private database handle data
 *           keysv - the attribute name
 *           valuesv - the attribute value
 *
 *  Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/
int
dbd_db_STORE_attrib(
                    SV* dbh,
                    imp_dbh_t* imp_dbh,
                    SV* keysv,
                    SV* valuesv
                   )
{
  STRLEN kl;
  char *key = SvPV(keysv, kl);
  SV *cachesv = Nullsv;
  int cacheit = FALSE;
  bool bool_value = SvTRUE(valuesv);

  if (kl==10 && strEQ(key, "AutoCommit"))
  {
    if (imp_dbh->has_transactions)
    {
      int oldval = DBIc_has(imp_dbh,DBIcf_AutoCommit);

      if (bool_value == oldval)
        return TRUE;

      if (drizzle_autocommit(imp_dbh->pdrizzle, bool_value))
      {
        do_error(dbh, TX_ERR_AUTOCOMMIT,
                 bool_value ?
                  "Turning on AutoCommit failed" :
                  "Turning off AutoCommit failed"
                 ,NULL);
        return FALSE;
      }
      DBIc_set(imp_dbh, DBIcf_AutoCommit, bool_value);
    }
    else
    {
      /*
       *  We do support neither transactions nor "AutoCommit".
       *  But we stub it. :-)
     */
      if (!SvTRUE(valuesv))
      {
        do_error(dbh, JW_ERR_NOT_IMPLEMENTED,
                 "Transactions not supported by database" ,NULL);
        croak("Transactions not supported by database");
      }
    }
  }
  else if (kl == 16 && strEQ(key,"drizzle_use_result"))
    imp_dbh->use_drizzle_use_result = bool_value;
  else if (kl == 20 && strEQ(key,"drizzle_auto_reconnect"))
    imp_dbh->auto_reconnect = bool_value;

  else if (kl == 31 && strEQ(key,"drizzle_unsafe_bind_type_guessing"))
	imp_dbh->bind_type_guessing = SvIV(valuesv);
  /*HELMUT */
#if defined(sv_utf8_decode)
  else if (kl == 17 && strEQ(key, "drizzle_enable_utf8"))
    imp_dbh->enable_utf8 = bool_value;
#endif
  else
    return FALSE;				/* Unknown key */

  if (cacheit) /* cache value for later DBI 'quick' fetch? */
    hv_store((HV*)SvRV(dbh), key, kl, cachesv, 0);
  return TRUE;
}

/***************************************************************************
 *
 *  Name:    dbd_db_FETCH_attrib
 *
 *  Purpose: Function for fetching dbh attributes
 *
 *  Input:   dbh - database handle being queried
 *           imp_dbh - drivers private database handle data
 *           keysv - the attribute name
 *
 *  Returns: An SV*, if sucessfull; NULL otherwise
 *
 *  Notes:   Do not forget to call sv_2mortal in the former case!
 *
 **************************************************************************/
SV* my_ulonglong2str(uint64_t val)
{
  char buf[64];
  char *ptr = buf + sizeof(buf) - 1;

  if (val == 0)
    return newSVpv("0", 1);

  *ptr = '\0';
  while (val > 0)
  {
    *(--ptr) = ('0' + (val % 10));
    val = val / 10;
  }
  return newSVpv(ptr, (buf+ sizeof(buf) - 1) - ptr);
}

SV* dbd_db_FETCH_attrib(SV *dbh, imp_dbh_t *imp_dbh, SV *keysv)
{
  STRLEN kl;
  char *key = SvPV(keysv, kl);
  char* fine_key = NULL;
  SV* result = NULL;
  dbh= dbh;

  switch (*key) {
    case 'A':
      if (strEQ(key, "AutoCommit"))
      {
        if (imp_dbh->has_transactions)
          return sv_2mortal(boolSV(DBIc_has(imp_dbh,DBIcf_AutoCommit)));
        /* Default */
        return &sv_yes;
      }
      break;
  }
  if (strncmp(key, "drizzle_", 8) == 0) {
    fine_key = key;
    key = key+6;
    kl = kl-6;
  }

  /* MONTY:  Check if kl should not be used or used everywhere */
  switch(*key) {
  case 'a':
    if (kl == strlen("auto_reconnect") && strEQ(key, "auto_reconnect"))
      result= sv_2mortal(newSViv(imp_dbh->auto_reconnect));
    break;
  case 'u':
    if (kl == strlen("unsafe_bind_type_guessing") &&
        strEQ(key, "unsafe_bind_type_guessing"))
      result = sv_2mortal(newSViv(imp_dbh->bind_type_guessing));
    break;
  case 'e':
    if (strEQ(key, "errno"))
      result= sv_2mortal(newSViv((IV)drizzle_errno(imp_dbh->pdrizzle)));
    else if ( strEQ(key, "error") || strEQ(key, "errmsg"))
    {
    /* Note that errmsg is obsolete, as of 2.09! */
      const char* msg = drizzle_error(imp_dbh->pdrizzle);
      result= sv_2mortal(newSVpv(msg, strlen(msg)));
    }
    /* HELMUT */
#if defined(sv_utf8_decode)
    else if (kl == strlen("enable_utf8") && strEQ(key, "enable_utf8"))
        result = sv_2mortal(newSViv(imp_dbh->enable_utf8));
#endif
    break;

  case 'd':
    if (strEQ(key, "dbd_stats"))
    {
      HV* hv = newHV();
      hv_store(
               hv,
               "auto_reconnects_ok",
               strlen("auto_reconnects_ok"),
               newSViv(imp_dbh->stats.auto_reconnects_ok),
               0
              );
      hv_store(
               hv,
               "auto_reconnects_failed",
               strlen("auto_reconnects_failed"),
               newSViv(imp_dbh->stats.auto_reconnects_failed),
               0
              );

      result= (newRV_noinc((SV*)hv));
    }

  case 'h':
    if (strEQ(key, "hostinfo"))
    {
      const char* hostinfo = drizzle_get_host_info(imp_dbh->pdrizzle);
      result= hostinfo ?
        sv_2mortal(newSVpv(hostinfo, strlen(hostinfo))) : &sv_undef;
    }
    break;

  case 'i':
    if (strEQ(key, "info"))
    {
      const char* info = drizzle_info(imp_dbh->pdrizzle);
      result= info ? sv_2mortal(newSVpv(info, strlen(info))) : &sv_undef;
    }
    else if (kl == 8  &&  strEQ(key, "insertid"))
      /* We cannot return an IV, because the insertid is a long. */
      result= sv_2mortal(my_ulonglong2str(drizzle_insert_id(imp_dbh->pdrizzle)));
    break;

  case 'p':
    if (kl == 9  &&  strEQ(key, "protoinfo"))
      result= sv_2mortal(newSViv(drizzle_get_proto_info(imp_dbh->pdrizzle)));
    break;

  case 's':
    if (kl == 10  &&  strEQ(key, "serverinfo"))
    {
      const char* serverinfo = drizzle_get_server_info(imp_dbh->pdrizzle);
      result= serverinfo ?
        sv_2mortal(newSVpv(serverinfo, strlen(serverinfo))) : &sv_undef;
    }
    else if (strEQ(key, "sock"))
      result= sv_2mortal(newSViv((IV) imp_dbh->pdrizzle));
    else if (strEQ(key, "sockfd"))
      result= sv_2mortal(newSViv((IV) imp_dbh->pdrizzle->net.fd));
#ifdef DRIZZLE_STAT
    else if (strEQ(key, "stat"))
    {
      const char* stats = drizzle_stat(imp_dbh->pdrizzle);
      result= stats ?
        sv_2mortal(newSVpv(stats, strlen(stats))) : &sv_undef;
    }
    else if (strEQ(key, "stats"))
    {
      /* Obsolete, as of 2.09 */
      const char* stats = drizzle_stat(imp_dbh->pdrizzle);
      result= stats ?
        sv_2mortal(newSVpv(stats, strlen(stats))) : &sv_undef;
    }
    break;
#endif
  case 't':
    if (kl == 9  &&  strEQ(key, "thread_id"))
      result= sv_2mortal(newSViv(drizzle_thread_id(imp_dbh->pdrizzle)));
    break;
  }

  if (result== NULL)
    return Nullsv;

  return result;
}


/* 
 **************************************************************************
 *
 *  Name:    dbd_st_prepare
 *
 *  Purpose: Called for preparing an SQL statement; our part of the
 *           statement handle constructor
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *           statement - pointer to string with SQL statement
 *           attribs - statement attributes, currently not in use
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error will
 *           be called in the latter case
 *
 **************************************************************************/
int
dbd_st_prepare(
  SV *sth,
  imp_sth_t *imp_sth,
  char *statement,
  SV *attribs)
{
  int i;
  SV **svp;
  D_imp_xxh(sth);
  D_imp_dbh_from_sth;


  imp_sth->fetch_done= 0;
  imp_sth->done_desc= 0;
  imp_sth->result= NULL;
  imp_sth->currow= 0;

  /* Set default value of 'drizzle_use_result' attribute for sth from dbh */
  svp= DBD_ATTRIB_GET_SVP(attribs, "drizzle_use_result", 16);
  imp_sth->use_drizzle_use_result= svp ?
    SvTRUE(*svp) : imp_dbh->use_drizzle_use_result;

  for (i= 0; i < AV_ATTRIB_LAST; i++)
    imp_sth->av_attr[i]= Nullav;

  /*
     Clean-up previous result set(s) for sth to prevent
     'Commands out of sync' error 
  */
  drizzle_st_free_result_sets(sth, imp_sth);

  DBIc_NUM_PARAMS(imp_sth) = count_params(statement);

  /* Allocate memory for parameters */
  imp_sth->params= alloc_param(DBIc_NUM_PARAMS(imp_sth));
  DBIc_IMPSET_on(imp_sth);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "\t<- dbd_st_prepare\n");
  return 1;
}

/***************************************************************************
 * Name: dbd_st_free_result_sets
 *
 * Purpose: Clean-up single or multiple result sets (if any)
 *
 * Inputs: sth - Statement handle
 *         imp_sth - driver's private statement handle
 *
 * Returns: 1 ok
 *          0 error
 *************************************************************************/
int drizzle_st_free_result_sets (SV * sth, imp_sth_t * imp_sth)
{
  D_imp_dbh_from_sth;
  D_imp_xxh(sth);
  int next_result_rc= -1;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "\t>- dbd_st_free_result_sets\n");

  do
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBILOGFP, "\t<- dbd_st_free_result_sets RC %d\n", next_result_rc);

    if (next_result_rc == 0)
    {
      if (!(imp_sth->result = drizzle_use_result(imp_dbh->pdrizzle)))
      {
        /* Check for possible error */
        if (drizzle_field_count(imp_dbh->pdrizzle))
        {
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBILOGFP, "\t<- dbd_st_free_result_sets ERROR: %s\n",
                                  drizzle_error(imp_dbh->pdrizzle));

          do_error(sth, drizzle_errno(imp_dbh->pdrizzle), drizzle_error(imp_dbh->pdrizzle),
                   drizzle_sqlstate(imp_dbh->pdrizzle));
          return 0;
        }
      }
    }
    if (imp_sth->result)
    {
      drizzle_free_result(imp_sth->result);
      imp_sth->result=NULL;
    }
  } while ((next_result_rc= drizzle_next_result(imp_dbh->pdrizzle))==0);

  if (next_result_rc > 0)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBILOGFP, "\t<- dbd_st_free_result_sets: Error while processing multi-result set: %s\n",
                    drizzle_error(imp_dbh->pdrizzle));

    do_error(sth, drizzle_errno(imp_dbh->pdrizzle), drizzle_error(imp_dbh->pdrizzle),
             drizzle_sqlstate(imp_dbh->pdrizzle));
  }


  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "\t<- dbd_st_free_result_sets\n");

  return 1;
}


/***************************************************************************
 * Name: dbd_st_more_results
 *
 * Purpose: Move onto the next result set (if any)
 *
 * Inputs: sth - Statement handle
 *         imp_sth - driver's private statement handle
 *
 * Returns: 1 if there are more results sets
 *          0 if there are not
 *         -1 for errors.
 *************************************************************************/
int dbd_st_more_results(SV* sth, imp_sth_t* imp_sth)
{
  D_imp_dbh_from_sth;
  D_imp_xxh(sth);

  int use_drizzle_use_result=imp_sth->use_drizzle_use_result;
  int next_result_return_code, i;
  DRIZZLE* con= imp_dbh->pdrizzle;

  if (!SvROK(sth) || SvTYPE(SvRV(sth)) != SVt_PVHV)
    croak("Expected hash array");

  if (!drizzle_more_results(con))
  {
    /* No more pending result set(s)*/
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBILOGFP,
		    "\n      <- dbs_st_more_rows no more results\n");
    return 0;
  }


  /*
   *  Free cached array attributes
   */
  for (i= 0; i < AV_ATTRIB_LAST;  i++)
  {
    if (imp_sth->av_attr[i])
      SvREFCNT_dec(imp_sth->av_attr[i]);

    imp_sth->av_attr[i]= Nullav;
  }

  /* Release previous MySQL result*/
  if (imp_sth->result)
    drizzle_free_result(imp_sth->result);

  if (DBIc_ACTIVE(imp_sth))
    DBIc_ACTIVE_off(imp_sth);

  next_result_return_code= drizzle_next_result(con);

  /*
    drizzle_next_result returns
      0 if there are more results
     -1 if there are no more results
     >0 if there was an error
   */
  if (next_result_return_code > 0)
  {
    do_error(sth, drizzle_errno(con), drizzle_error(con),
             drizzle_sqlstate(con));
    return 0;
  }
  else
  {
    /* Store the result from the Query */
    imp_sth->result = use_drizzle_use_result ?
     drizzle_use_result(con) : drizzle_store_result(con);

    if (drizzle_errno(con))
      do_error(sth, drizzle_errno(con), drizzle_error(con),
               drizzle_sqlstate(con));

    if (imp_sth->result == NULL)
    {
      /* No "real" rowset*/
      return 0;
    }
    else
    {
      /* We have a new rowset */
      imp_sth->currow=0;


      /* delete cached handle attributes */
      /* XXX should be driven by a list to ease maintenance */
      hv_delete((HV*)SvRV(sth), "NAME", 4, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "NULLABLE", 8, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "NUM_OF_FIELDS", 13, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "PRECISION", 9, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "SCALE", 5, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "TYPE", 4, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_insertid", 14, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_is_auto_increment", 23, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_is_blob", 13, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_is_key", 12, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_is_num", 12, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_is_pri_key", 16, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_length", 12, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_max_length", 16, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_table", 11, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_type", 10, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_type_name", 15, G_DISCARD);
      hv_delete((HV*)SvRV(sth), "drizzle_warning_count", 20, G_DISCARD);

      /* Adjust NUM_OF_FIELDS - which also adjusts the row buffer size */
      DBIc_NUM_FIELDS(imp_sth)= 0; /* for DBI <= 1.53 */
      DBIS->set_attr_k(sth, sv_2mortal(newSVpvn("NUM_OF_FIELDS",13)), 0,
          sv_2mortal(newSViv(drizzle_num_fields(imp_sth->result)))
      );

      DBIc_ACTIVE_on(imp_sth);

      imp_sth->done_desc = 0;
    }
    imp_dbh->pdrizzle->net.last_errno= 0;
    return 1;
  }
}
/**************************************************************************
 *
 *  Name:    drizzle_st_internal_execute
 *
 *  Purpose: Internal version for executing a statement, called both from
 *           within the "do" and the "execute" method.
 *
 *  Inputs:  h - object handle, for storing error messages
 *           statement - query being executed
 *           attribs - statement attributes, currently ignored
 *           num_params - number of parameters being bound
 *           params - parameter array
 *           result - where to store results, if any
 *           con - connection to the database
 *
 **************************************************************************/


uint64_t drizzle_st_internal_execute(
                                       SV *h, /* could be sth or dbh */
                                       SV *statement,
                                       SV *attribs,
                                       int num_params,
                                       imp_sth_ph_t *params,
                                       DRIZZLE_RES **result,
                                       DRIZZLE *con,
                                       int use_drizzle_use_result
                                      )
{
  bool bind_type_guessing;
  STRLEN slen;
  char *sbuf = SvPV(statement, slen);
  char *table;
  char *salloc;
  int htype;
  int errno;
  uint64_t rows= 0;
  /* thank you DBI.c for this info! */
  D_imp_xxh(h);
  attribs= attribs;

  htype= DBIc_TYPE(imp_xxh);
  /*
    It is important to import imp_dbh properly according to the htype
    that it is! Also, one might ask why bind_type_guessing is assigned
    in each block. Well, it's because D_imp_ macros called in these
    blocks make it so imp_dbh is not "visible" or defined outside of the
    if/else (when compiled, it fails for imp_dbh not being defined).
  */
  /* h is a dbh */
  if (htype==DBIt_DB)
  {
    D_imp_dbh(h);
    /* if imp_dbh is not available, it causes segfault (proper) on OpenBSD */
    if (imp_dbh)
      bind_type_guessing= imp_dbh->bind_type_guessing;
    else
      bind_type_guessing=0;
  }
  /* h is a sth */
  else
  {
    D_imp_sth(h);
    D_imp_dbh_from_sth;
    /* if imp_dbh is not available, it causes segfault (proper) on OpenBSD */
    if (imp_dbh)
      bind_type_guessing= imp_dbh->bind_type_guessing;
    else
      bind_type_guessing=0;
  }

  salloc= parse_params(con,
                              sbuf,
                              &slen,
                              params,
                              num_params,
                              bind_type_guessing);

  if (salloc)
  {
    sbuf= salloc;
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBILOGFP, "Binding parameters: %s\n", sbuf);
  }

  if (slen >= 11 && (!strncmp(sbuf, "listfields ", 11) ||
                     !strncmp(sbuf, "LISTFIELDS ", 11)))
  {
    /* remove pre-space */
    slen-= 10;
    sbuf+= 10;
    while (slen && isspace(*sbuf)) { --slen;  ++sbuf; }

    if (!slen)
    {
      do_error(h, JW_ERR_QUERY, "Missing table name" ,NULL);
      return -2;
    }
    if (!(table= malloc(slen+1)))
    {
      do_error(h, JW_ERR_MEM, "Out of memory" ,NULL);
      return -2;
    }

    strncpy(table, sbuf, slen);
    sbuf= table;

    while (slen && !isspace(*sbuf))
    {
      --slen;
      ++sbuf;
    }
    *sbuf++= '\0';

    *result= drizzle_list_fields(con, table, NULL);

    free(table);

    if (!(*result))
    {
      do_error(h, drizzle_errno(con), drizzle_error(con)
               ,drizzle_sqlstate(con));
      return -2;
    }

    return 0;
  }

  if ((drizzle_real_query(con, sbuf, slen))  &&
      (!drizzle_db_reconnect(h)  ||
       (drizzle_real_query(con, sbuf, slen))))
  {
    Safefree(salloc);
    do_error(h, drizzle_errno(con), drizzle_error(con),
             drizzle_sqlstate(con));
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBILOGFP, "IGNORING ERROR errno %d\n", errno);
    return -2;
  }
  Safefree(salloc);

  /** Store the result from the Query */
  *result= use_drizzle_use_result ?
    drizzle_use_result(con) : drizzle_store_result(con);

  if (drizzle_errno(con))
    do_error(h, drizzle_errno(con), drizzle_error(con)
             ,drizzle_sqlstate(con));

  if (!*result)
    rows= drizzle_affected_rows(con);
  else
    rows= drizzle_num_rows(*result);

  return(rows);
}


/***************************************************************************
 *
 *  Name:    dbd_st_execute
 *
 *  Purpose: Called for preparing an SQL statement; our part of the
 *           statement handle constructor
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error will
 *           be called in the latter case
 *
 **************************************************************************/

int dbd_st_execute(SV* sth, imp_sth_t* imp_sth)
{
  char actual_row_num[64];
  int i;
  SV **statement;
  D_imp_dbh_from_sth;
  D_imp_xxh(sth);
#if defined (dTHR)
  dTHR;
#endif

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP,
      " -> dbd_st_execute for %08lx\n", (u_long) sth);

  if (!SvROK(sth)  ||  SvTYPE(SvRV(sth)) != SVt_PVHV)
    croak("Expected hash array");

  /* Free cached array attributes */
  for (i= 0;  i < AV_ATTRIB_LAST;  i++)
  {
    if (imp_sth->av_attr[i])
      SvREFCNT_dec(imp_sth->av_attr[i]);

    imp_sth->av_attr[i]= Nullav;
  }

  statement= hv_fetch((HV*) SvRV(sth), "Statement", 9, FALSE);

  /* 
     Clean-up previous result set(s) for sth to prevent
     'Commands out of sync' error 
  */
  drizzle_st_free_result_sets (sth, imp_sth);

  imp_sth->row_num= drizzle_st_internal_execute(
                                                sth,
                                                *statement,
                                                NULL,
                                                DBIc_NUM_PARAMS(imp_sth),
                                                imp_sth->params,
                                                &imp_sth->result,
                                                imp_dbh->pdrizzle,
                                                imp_sth->use_drizzle_use_result
                                               );

  if (imp_sth->row_num+1 != (uint64_t )-1)
  {
    if (!imp_sth->result)
      imp_sth->insertid= drizzle_insert_id(imp_dbh->pdrizzle);
    else
    {
      /** Store the result in the current statement handle */
      DBIc_NUM_FIELDS(imp_sth)= drizzle_num_fields(imp_sth->result);
      DBIc_ACTIVE_on(imp_sth);
      imp_sth->done_desc= 0;
      imp_sth->fetch_done= 0;
    }
  }

  imp_sth->warning_count = drizzle_warning_count(imp_dbh->pdrizzle);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
  {
    /* 
      PerlIO_printf doesn't always handle imp_sth->row_num %llu 
      consistantly!!
    */
    sprintf(actual_row_num, "%llu", imp_sth->row_num);
    PerlIO_printf(DBILOGFP,
                  " <- dbd_st_execute returning imp_sth->row_num %s\n",
                  actual_row_num);
  }

  return (int)imp_sth->row_num;
}

 /**************************************************************************
 *
 *  Name:    dbd_describe
 *
 *  Purpose: Called from within the fetch method to describe the result
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - our part of the statement handle, there's no
 *               need for supplying both; Tim just doesn't remove it
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error will
 *           be called in the latter case
 *
 **************************************************************************/

int dbd_describe(SV* sth, imp_sth_t* imp_sth)
{
  D_imp_xxh(sth);
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "\t--> dbd_describe\n");

  imp_sth->done_desc= 1;
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "\t<- dbd_describe\n");
  return TRUE;
}

/**************************************************************************
 *
 *  Name:    dbd_st_fetch
 *
 *  Purpose: Called for fetching a result row
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: array of columns; the array is allocated by DBI via
 *           DBIS->get_fbav(imp_sth), even the values of the array
 *           are prepared, we just need to modify them appropriately
 *
 **************************************************************************/

AV*
dbd_st_fetch(SV *sth, imp_sth_t* imp_sth)
{
  int num_fields, ChopBlanks, i;
  unsigned long *lengths;
  AV *av;
  int av_length, av_readonly;
  DRIZZLE_ROW cols;
  D_imp_dbh_from_sth;
  DRIZZLE *con= imp_dbh->pdrizzle;
  D_imp_xxh(sth);
  DRIZZLE_FIELD *fields;
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "\t-> dbd_st_fetch\n");


  ChopBlanks = DBIc_is(imp_sth, DBIcf_ChopBlanks);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t\tdbd_st_fetch for %08lx, chopblanks %d\n",
                  (u_long) sth, ChopBlanks);

  if (!imp_sth->result)
  {
    do_error(sth, JW_ERR_SEQUENCE, "fetch() without execute()" ,NULL);
    return Nullav;
  }

  /* fix from 2.9008 */
  imp_dbh->pdrizzle->net.last_errno = 0;

  imp_sth->currow++;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
  {
    PerlIO_printf(DBILOGFP, "\tdbd_st_fetch result set details\n");
    PerlIO_printf(DBILOGFP, "\timp_sth->result=%08lx\n",imp_sth->result);
    PerlIO_printf(DBILOGFP, "\tdrizzle_num_fields=%llu\n",
                  drizzle_num_fields(imp_sth->result));
    PerlIO_printf(DBILOGFP, "\tdrizzle_num_rows=%llu\n",
                  drizzle_num_rows(imp_sth->result));
    PerlIO_printf(DBILOGFP, "\tdrizzle_affected_rows=%llu\n",
                  drizzle_affected_rows(imp_dbh->pdrizzle));
    PerlIO_printf(DBILOGFP, "\tdbd_st_fetch for %08lx, currow= %d\n",
                  (u_long) sth,imp_sth->currow);
  }

  if (!(cols= drizzle_fetch_row(imp_sth->result)))
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    {
      PerlIO_printf(DBILOGFP, "\tdbd_st_fetch, no more rows to fetch");
    }
    if (drizzle_errno(imp_dbh->pdrizzle))
      do_error(sth, drizzle_errno(imp_dbh->pdrizzle),
               drizzle_error(imp_dbh->pdrizzle),
               drizzle_sqlstate(imp_dbh->pdrizzle));


    if (!drizzle_more_results(con))
      dbd_st_finish(sth, imp_sth);
    return Nullav;
  }

  num_fields= drizzle_num_fields(imp_sth->result);
  fields= drizzle_fetch_fields(imp_sth->result);
  lengths= drizzle_fetch_lengths(imp_sth->result);

  if ((av= DBIc_FIELDS_AV(imp_sth)) != Nullav)
  {
    av_length= av_len(av)+1;

    if (av_length != num_fields)              /* Resize array if necessary */
    {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBILOGFP, "\t<- dbd_st_fetch, size of results array(%d) != num_fields(%d)\n",
                      av_length, num_fields);

      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBILOGFP, "\t<- dbd_st_fetch, result fields(%d)\n",
                      DBIc_NUM_FIELDS(imp_sth));

      av_readonly = SvREADONLY(av);

      if (av_readonly)
        SvREADONLY_off( av );              /* DBI sets this readonly */

      while (av_length < num_fields)
      {
        av_store(av, av_length++, newSV(0));
      }

      while (av_length > num_fields)
      {
        SvREFCNT_dec(av_pop(av));
        av_length--;
      }
      if (av_readonly)
        SvREADONLY_on(av);
    }
  }

  av= DBIS->get_fbav(imp_sth);

  for (i= 0;  i < num_fields; ++i)
  {
    char *col= cols[i];
    SV *sv= AvARRAY(av)[i]; /* Note: we (re)use the SV in the AV	*/

    if (col)
    {
      STRLEN len= lengths[i];
      if (ChopBlanks)
      {
        while (len && col[len-1] == ' ')
        {	--len; }
      }
      sv_setpvn(sv, col, len);
      /* UTF8 */
      /*HELMUT*/
#if defined(sv_utf8_decode)

      if (imp_dbh->enable_utf8 && fields[i].charsetnr != 63)
        if (imp_dbh->enable_utf8 && !(fields[i].flags & BINARY_FLAG))
#endif
          sv_utf8_decode(sv);
      /* END OF UTF8 */
    }
    else
      (void) SvOK_off(sv);  /*  Field is NULL, return undef  */
  }

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP, "\t<- dbd_st_fetch, %d cols\n", num_fields);
  return av;

}

/***************************************************************************
 *
 *  Name:    dbd_st_finish
 *
 *  Purpose: Called for freeing a drizzle result
 *
 *  Input:   sth - statement handle being finished
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error() will
 *           be called in the latter case
 *
 **************************************************************************/

int dbd_st_finish(SV* sth, imp_sth_t* imp_sth) {
  D_imp_xxh(sth);

#if defined (dTHR)
  dTHR;
#endif

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
  {
    PerlIO_printf(DBILOGFP, "\n--> dbd_st_finish\n");
  }


  /*
    Cancel further fetches from this cursor.
    We don't close the cursor till DESTROY.
    The application may re execute it.
  */
  if (imp_sth && imp_sth->result)
  {
    /*
      Clean-up previous result set(s) for sth to prevent
      'Commands out of sync' error
    */
    drizzle_st_free_result_sets(sth, imp_sth);
  }
  DBIc_ACTIVE_off(imp_sth);
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
  {
    PerlIO_printf(DBILOGFP, "\n<-- dbd_st_finish\n");
  }
  return 1;
}


/**************************************************************************
 *
 *  Name:    dbd_st_destroy
 *
 *  Purpose: Our part of the statement handles destructor
 *
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void dbd_st_destroy(SV *sth, imp_sth_t *imp_sth) {
  D_imp_xxh(sth);

#if defined (dTHR)
  dTHR;
#endif

  int i;

  /* Free values allocated by dbd_bind_ph */
  if (imp_sth->params)
  {
    free_param(imp_sth->params, DBIc_NUM_PARAMS(imp_sth));
    imp_sth->params= NULL;
  }

  /* Free cached array attributes */
  for (i= 0; i < AV_ATTRIB_LAST; i++)
  {
    if (imp_sth->av_attr[i])
      SvREFCNT_dec(imp_sth->av_attr[i]);
    imp_sth->av_attr[i]= Nullav;
  }
  /* let DBI know we've done it   */
  DBIc_IMPSET_off(imp_sth);
}


/*
 **************************************************************************
 *
 *  Name:    dbd_st_STORE_attrib
 *
 *  Purpose: Modifies a statement handles attributes; we currently
 *           support just nothing
 *
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *           keysv - attribute name
 *           valuesv - attribute value
 *
 *  Returns: TRUE for success, FALSE otrherwise; do_error will
 *           be called in the latter case
 *
 **************************************************************************/
int
dbd_st_STORE_attrib(
                    SV *sth,
                    imp_sth_t *imp_sth,
                    SV *keysv,
                    SV *valuesv
                   )
{
  STRLEN(kl);
  char *key= SvPV(keysv, kl);
  int retval= FALSE;
  D_imp_xxh(sth);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t\t-> dbd_st_STORE_attrib for %08lx, key %s\n",
                  (u_long) sth, key);

  if (strEQ(key, "drizzle_use_result"))
  {
    imp_sth->use_drizzle_use_result= SvTRUE(valuesv);
  }

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t\t<- dbd_st_STORE_attrib for %08lx, result %d\n",
                  (u_long) sth, retval);

  return retval;
}


/*
 **************************************************************************
 *
 *  Name:    dbd_st_FETCH_internal
 *
 *  Purpose: Retrieves a statement handles array attributes; we use
 *           a separate function, because creating the array
 *           attributes shares much code and it aids in supporting
 *           enhanced features like caching.
 *
 *  Input:   sth - statement handle; may even be a database handle,
 *               in which case this will be used for storing error
 *               messages only. This is only valid, if cacheit (the
 *               last argument) is set to TRUE.
 *           what - internal attribute number
 *           res - pointer to a DBMS result
 *           cacheit - TRUE, if results may be cached in the sth.
 *
 *  Returns: RV pointing to result array in case of success, NULL
 *           otherwise; do_error has already been called in the latter
 *           case.
 *
 **************************************************************************/

#ifndef IS_KEY
#define IS_KEY(A) (((A) & (PRI_KEY_FLAG | UNIQUE_KEY_FLAG | MULTIPLE_KEY_FLAG)) != 0)
#endif

#if !defined(IS_AUTO_INCREMENT) && defined(AUTO_INCREMENT_FLAG)
#define IS_AUTO_INCREMENT(A) (((A) & AUTO_INCREMENT_FLAG) != 0)
#endif

SV*
dbd_st_FETCH_internal(
  SV *sth,
  int what,
  DRIZZLE_RES *res,
  int cacheit
)
{
  D_imp_sth(sth);
  AV *av= Nullav;
  DRIZZLE_FIELD *curField;

  /* Are we asking for a legal value? */
  if (what < 0 ||  what >= AV_ATTRIB_LAST)
    do_error(sth, JW_ERR_NOT_IMPLEMENTED, "Not implemented", NULL);

  /* Return cached value, if possible */
  else if (cacheit  &&  imp_sth->av_attr[what])
    av= imp_sth->av_attr[what];

  /* Does this sth really have a result? */
  else if (!res)
    do_error(sth, JW_ERR_NOT_ACTIVE,
	     "statement contains no result" ,NULL);
  /* Do the real work. */
  else
  {
    av= newAV();
    drizzle_field_seek(res, 0);
    while ((curField= drizzle_fetch_field(res)))
    {
      SV *sv;

      switch(what) {
      case AV_ATTRIB_NAME:
        sv= newSVpv(curField->name, strlen(curField->name));
        break;

      case AV_ATTRIB_TABLE:
        sv= newSVpv(curField->table, strlen(curField->table));
        break;

      case AV_ATTRIB_TYPE:
        sv= newSViv((int) curField->type);
        break;

      case AV_ATTRIB_SQL_TYPE:
        sv= newSViv((int) native2sql(curField->type)->data_type);
        break;
      case AV_ATTRIB_IS_PRI_KEY:
        sv= boolSV(IS_PRI_KEY(curField->flags));
        break;

      case AV_ATTRIB_IS_NOT_NULL:
        sv= boolSV(IS_NOT_NULL(curField->flags));
        break;

      case AV_ATTRIB_NULLABLE:
        sv= boolSV(!IS_NOT_NULL(curField->flags));
        break;

      case AV_ATTRIB_LENGTH:
        sv= newSViv((int) curField->length);
        break;

      case AV_ATTRIB_IS_NUM:
        sv= newSViv((int) native2sql(curField->type)->is_num);
        break;

      case AV_ATTRIB_TYPE_NAME:
        sv= newSVpv((char*) native2sql(curField->type)->type_name, 0);
        break;

      case AV_ATTRIB_MAX_LENGTH:
        sv= newSViv((int) curField->max_length);
        break;

      case AV_ATTRIB_IS_AUTO_INCREMENT:
#if defined(AUTO_INCREMENT_FLAG)
        sv= boolSV(IS_AUTO_INCREMENT(curField->flags));
        break;
#else
        croak("AUTO_INCREMENT_FLAG is not supported on this machine");
#endif

      case AV_ATTRIB_IS_KEY:
        sv= boolSV(IS_KEY(curField->flags));
        break;

      case AV_ATTRIB_IS_BLOB:
        sv= boolSV(IS_BLOB(curField->flags));
        break;

      case AV_ATTRIB_SCALE:
        sv= newSViv((int) curField->decimals);
        break;

      case AV_ATTRIB_PRECISION:
        sv= newSViv((int) (curField->length > curField->max_length) ?
                     curField->length : curField->max_length);
        break;

      default:
        sv= &sv_undef;
        break;
      }
      av_push(av, sv);
    }

    /* Ensure that this value is kept, decremented in
     *  dbd_st_destroy and dbd_st_execute.  */
    if (!cacheit)
      return sv_2mortal(newRV_noinc((SV*)av));
    imp_sth->av_attr[what]= av;
  }

  if (av == Nullav)
    return &sv_undef;

  return sv_2mortal(newRV_inc((SV*)av));
}


/*
 **************************************************************************
 *
 *  Name:    dbd_st_FETCH_attrib
 *
 *  Purpose: Retrieves a statement handles attributes
 *
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *           keysv - attribute name
 *
 *  Returns: NULL for an unknown attribute, "undef" for error,
 *           attribute value otherwise.
 *
 **************************************************************************/

#define ST_FETCH_AV(what) \
    dbd_st_FETCH_internal(sth, (what), imp_sth->result, TRUE)

  SV* dbd_st_FETCH_attrib(
                          SV *sth,
                          imp_sth_t *imp_sth,
                          SV *keysv
                         )
{
  STRLEN(kl);
  char *key= SvPV(keysv, kl);
  SV *retsv= Nullsv;
  D_imp_xxh(sth);

  if (kl < 2)
    return Nullsv;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBILOGFP,
                  "    -> dbd_st_FETCH_attrib for %08lx, key %s\n",
                  (u_long) sth, key);

  switch (*key) {
  case 'N':
    if (strEQ(key, "NAME"))
      retsv= ST_FETCH_AV(AV_ATTRIB_NAME);
    else if (strEQ(key, "NULLABLE"))
      retsv= ST_FETCH_AV(AV_ATTRIB_NULLABLE);
    break;
  case 'P':
    if (strEQ(key, "PRECISION"))
      retsv= ST_FETCH_AV(AV_ATTRIB_PRECISION);
    if (strEQ(key, "ParamValues"))
    {
        HV *pvhv= newHV();
        if (DBIc_NUM_PARAMS(imp_sth))
        {
            int n;
            char key[100];
            I32 keylen;
            for (n= 0; n < DBIc_NUM_PARAMS(imp_sth); n++)
            {
                keylen= sprintf(key, "%d", n);
                hv_store(pvhv, key,
                         keylen, newSVsv(imp_sth->params[n].value), 0);
            }
        }
        retsv= newRV_noinc((SV*)pvhv);
    }
    break;
  case 'S':
    if (strEQ(key, "SCALE"))
      retsv= ST_FETCH_AV(AV_ATTRIB_SCALE);
    break;
  case 'T':
    if (strEQ(key, "TYPE"))
      retsv= ST_FETCH_AV(AV_ATTRIB_SQL_TYPE);
    break;
  case 'm':
    switch (kl) {
    case 10:
      if (strEQ(key, "drizzle_type"))
        retsv= ST_FETCH_AV(AV_ATTRIB_TYPE);
      break;
    case 11:
      if (strEQ(key, "drizzle_table"))
        retsv= ST_FETCH_AV(AV_ATTRIB_TABLE);
      break;
    case 12:
      if (       strEQ(key, "drizzle_is_key"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_KEY);
      else if (strEQ(key, "drizzle_is_num"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_NUM);
      else if (strEQ(key, "drizzle_length"))
        retsv= ST_FETCH_AV(AV_ATTRIB_LENGTH);
      else if (strEQ(key, "drizzle_result"))
        retsv= sv_2mortal(newSViv((IV) imp_sth->result));
      break;
    case 13:
      if (strEQ(key, "drizzle_is_blob"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_BLOB);
      break;
    case 14:
      if (strEQ(key, "drizzle_insertid"))
      {
        /* We cannot return an IV, because the insertid is a long.  */
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBILOGFP, "INSERT ID %d\n", imp_sth->insertid);

        return sv_2mortal(my_ulonglong2str(imp_sth->insertid));
      }
      break;
    case 15:
      if (strEQ(key, "drizzle_type_name"))
        retsv = ST_FETCH_AV(AV_ATTRIB_TYPE_NAME);
      break;
    case 16:
      if ( strEQ(key, "drizzle_is_pri_key"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_PRI_KEY);
      else if (strEQ(key, "drizzle_max_length"))
        retsv= ST_FETCH_AV(AV_ATTRIB_MAX_LENGTH);
      else if (strEQ(key, "drizzle_use_result"))
        retsv= boolSV(imp_sth->use_drizzle_use_result);
      break;
    case 19:
      if (strEQ(key, "drizzle_warning_count"))
        retsv= sv_2mortal(newSViv((IV) imp_sth->warning_count));
      break;
    case 23:
      if (strEQ(key, "drizzle_is_auto_increment"))
        retsv = ST_FETCH_AV(AV_ATTRIB_IS_AUTO_INCREMENT);
      break;
    }
    break;
  }
  return retsv;
}


/***************************************************************************
 *
 *  Name:    dbd_st_blob_read
 *
 *  Purpose: Used for blob reads if the statement handles "LongTruncOk"
 *           attribute (currently not supported by DBD::drizzle)
 *
 *  Input:   SV* - statement handle from which a blob will be fetched
 *           imp_sth - drivers private statement handle data
 *           field - field number of the blob (note, that a row may
 *               contain more than one blob)
 *           offset - the offset of the field, where to start reading
 *           len - maximum number of bytes to read
 *           destrv - RV* that tells us where to store
 *           destoffset - destination offset
 *
 *  Returns: TRUE for success, FALSE otrherwise; do_error will
 *           be called in the latter case
 *
 **************************************************************************/

int dbd_st_blob_read (
  SV *sth,
  imp_sth_t *imp_sth,
  int field,
  long offset,
  long len,
  SV *destrv,
  long destoffset)
{
    /* quell warnings */
    sth= sth;
    imp_sth=imp_sth;
    field= field;
    offset= offset;
    len= len;
    destrv= destrv;
    destoffset= destoffset;
    return FALSE;
}


/***************************************************************************
 *
 *  Name:    dbd_bind_ph
 *
 *  Purpose: Binds a statement value to a parameter
 *
 *  Input:   sth - statement handle
 *           imp_sth - drivers private statement handle data
 *           param - parameter number, counting starts with 1
 *           value - value being inserted for parameter "param"
 *           sql_type - SQL type of the value
 *           attribs - bind parameter attributes, currently this must be
 *               one of the values SQL_CHAR, ...
 *           inout - TRUE, if parameter is an output variable (currently
 *               this is not supported)
 *           maxlen - ???
 *
 *  Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int dbd_bind_ph (SV *sth, imp_sth_t *imp_sth, SV *param, SV *value,
		 IV sql_type, SV *attribs, int is_inout, IV maxlen) {
  int rc;
  int param_num= SvIV(param);
  int idx= param_num - 1;
  char err_msg[64];
  D_imp_xxh(sth);

  STRLEN slen;
  char *buffer= NULL;
  int buffer_is_null= 0;
  int buffer_length= slen;
  unsigned int buffer_type= 0;
  attribs= attribs;
  maxlen= maxlen;

  if (param_num <= 0  ||  param_num > DBIc_NUM_PARAMS(imp_sth))
  {
    do_error(sth, JW_ERR_ILLEGAL_PARAM_NUM, "Illegal parameter number", NULL);
    return FALSE;
  }

  /*
     This fixes the bug whereby no warning was issued upone binding a
     defined non-numeric as numeric
   */
  if (SvOK(value) &&
      (sql_type == SQL_NUMERIC  ||
       sql_type == SQL_DECIMAL  ||
       sql_type == SQL_INTEGER  ||
       sql_type == SQL_SMALLINT ||
       sql_type == SQL_FLOAT    ||
       sql_type == SQL_REAL     ||
       sql_type == SQL_DOUBLE) )
  {
    if (! looks_like_number(value))
    {
      sprintf(err_msg,
              "Binding non-numeric field %d, value %s as a numeric!",
              param_num, neatsvpv(value,0));
      do_error(sth, JW_ERR_ILLEGAL_PARAM_NUM, err_msg, NULL);
    }
  }

  if (is_inout)
  {
    do_error(sth, JW_ERR_NOT_IMPLEMENTED, "Output parameters not implemented", NULL);
    return FALSE;
  }

  rc = bind_param(&imp_sth->params[idx], value, sql_type);

  return rc;
}


/***************************************************************************
 *
 *  Name:    drizzle_db_reconnect
 *
 *  Purpose: If the server has disconnected, try to reconnect.
 *
 *  Input:   h - database or statement handle
 *
 *  Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int drizzle_db_reconnect(SV* h)
{
  D_imp_xxh(h);
  imp_dbh_t* imp_dbh;
  DRIZZLE con;

  if (DBIc_TYPE(imp_xxh) == DBIt_ST)
  {
    imp_dbh = (imp_dbh_t*) DBIc_PARENT_COM(imp_xxh);
    h = DBIc_PARENT_H(imp_xxh);
  }
  else
    imp_dbh= (imp_dbh_t*) imp_xxh;

  if (drizzle_errno(imp_dbh->pdrizzle) != CR_SERVER_GONE_ERROR)
    /* Other error */
    return FALSE;

  if (!DBIc_has(imp_dbh, DBIcf_AutoCommit) || !imp_dbh->auto_reconnect)
  {
    /* We never reconnect if AutoCommit is turned off.
     * Otherwise we might get an inconsistent transaction
     * state.
     */
    return FALSE;
  }

  /* my_login will blow away imp_dbh->drizzle so we save a copy of
   * imp_dbh->drizzle and put it back where it belongs if the reconnect
   * fail.  Think server is down & reconnect fails but the application eval{}s
   * the execute, so next time $dbh->quote() gets called, instant SIGSEGV!
   */
  con= *(imp_dbh->pdrizzle);
  memcpy (&con, imp_dbh->pdrizzle,sizeof(con));
  memset (imp_dbh->pdrizzle,0,sizeof(*(imp_dbh->pdrizzle)));

  if (!my_login(h, imp_dbh))
  {
    do_error(h, drizzle_errno(imp_dbh->pdrizzle), drizzle_error(imp_dbh->pdrizzle),
             drizzle_sqlstate(imp_dbh->pdrizzle));
    memcpy (imp_dbh->pdrizzle, &con, sizeof(con));
    ++imp_dbh->stats.auto_reconnects_failed;
    return FALSE;
  }
  ++imp_dbh->stats.auto_reconnects_ok;
  return TRUE;
}


/**************************************************************************
 *
 *  Name:    dbd_db_type_info_all
 *
 *  Purpose: Implements $dbh->type_info_all
 *
 *  Input:   dbh - database handle
 *           imp_sth - drivers private database handle data
 *
 *  Returns: RV to AV of types
 *
 **************************************************************************/

#define PV_PUSH(c)                              \
    if (c) {                                    \
	sv= newSVpv((char*) (c), 0);           \
	SvREADONLY_on(sv);                      \
    } else {                                    \
        sv= &sv_undef;                         \
    }                                           \
    av_push(row, sv);

#define IV_PUSH(i) sv= newSViv((i)); SvREADONLY_on(sv); av_push(row, sv);

AV *dbd_db_type_info_all(SV *dbh, imp_dbh_t *imp_dbh)
{
  AV *av= newAV();
  AV *row;
  HV *hv;
  SV *sv;
  int i;
  const char *cols[] = {
    "TYPE_NAME",
    "DATA_TYPE",
    "COLUMN_SIZE",
    "LITERAL_PREFIX",
    "LITERAL_SUFFIX",
    "CREATE_PARAMS",
    "NULLABLE",
    "CASE_SENSITIVE",
    "SEARCHABLE",
    "UNSIGNED_ATTRIBUTE",
    "FIXED_PREC_SCALE",
    "AUTO_UNIQUE_VALUE",
    "LOCAL_TYPE_NAME",
    "MINIMUM_SCALE",
    "MAXIMUM_SCALE",
    "NUM_PREC_RADIX",
    "SQL_DATATYPE",
    "SQL_DATETIME_SUB",
    "INTERVAL_PRECISION",
    "drizzle_native_type",
    "drizzle_is_num"
  };

  dbh= dbh;
  imp_dbh= imp_dbh;
 
  hv= newHV();
  av_push(av, newRV_noinc((SV*) hv));
  for (i= 0;  i < (int)(sizeof(cols) / sizeof(const char*));  i++)
  {
    if (!hv_store(hv, (char*) cols[i], strlen(cols[i]), newSViv(i), 0))
    {
      SvREFCNT_dec((SV*) av);
      return Nullav;
    }
  }
  for (i= 0;  i < (int)SQL_GET_TYPE_INFO_num;  i++)
  {
    const sql_type_info_t *t= &SQL_GET_TYPE_INFO_values[i];

    row= newAV();
    av_push(av, newRV_noinc((SV*) row));
    PV_PUSH(t->type_name);
    IV_PUSH(t->data_type);
    IV_PUSH(t->column_size);
    PV_PUSH(t->literal_prefix);
    PV_PUSH(t->literal_suffix);
    PV_PUSH(t->create_params);
    IV_PUSH(t->nullable);
    IV_PUSH(t->case_sensitive);
    IV_PUSH(t->searchable);
    IV_PUSH(t->unsigned_attribute);
    IV_PUSH(t->fixed_prec_scale);
    IV_PUSH(t->auto_unique_value);
    PV_PUSH(t->local_type_name);
    IV_PUSH(t->minimum_scale);
    IV_PUSH(t->maximum_scale);

    if (t->num_prec_radix)
    {
      IV_PUSH(t->num_prec_radix);
    }
    else
      av_push(row, &sv_undef);

    IV_PUSH(t->sql_datatype); /* SQL_DATATYPE*/
    IV_PUSH(t->sql_datetime_sub); /* SQL_DATETIME_SUB*/
    IV_PUSH(t->interval_precision); /* INTERVAL_PERCISION */
    IV_PUSH(t->native_type);
    IV_PUSH(t->is_num);
  }
  return av;
}


/*
  dbd_db_quote

  Properly quotes a value 
*/
SV* dbd_db_quote(SV *dbh, SV *str, SV *type)
{
  SV *result;

  if (SvGMAGICAL(str))
    mg_get(str);

  if (!SvOK(str))
    result= newSVpv("NULL", 4);
  else
  {
    char *ptr, *sptr;
    STRLEN len;

    D_imp_dbh(dbh);

    if (type && SvMAGICAL(type))
      mg_get(type);

    if (type  &&  SvOK(type))
    {
      int i;
      int tp= SvIV(type);
      for (i= 0;  i < (int)SQL_GET_TYPE_INFO_num;  i++)
      {
        const sql_type_info_t *t= &SQL_GET_TYPE_INFO_values[i];
        if (t->data_type == tp)
        {
          if (!t->literal_prefix)
            return Nullsv;
          break;
        }
      }
    }

    ptr= SvPV(str, len);
    result= newSV(len*2+3);
#ifdef SvUTF8
    if (SvUTF8(str)) SvUTF8_on(result);
#endif
    sptr= SvPVX(result);

    *sptr++ = '\'';
    sptr+= drizzle_escape_string(sptr,
                                     ptr, len);
    *sptr++= '\'';
    SvPOK_on(result);
    SvCUR_set(result, sptr - SvPVX(result));
    /* Never hurts NUL terminating a Per string */
    *sptr++= '\0';
  }
  return result;
}

#ifdef DBD_DRIZZLE_INSERT_ID_IS_GOOD
SV *drizzle_db_last_insert_id(SV *dbh, imp_dbh_t *imp_dbh,
        SV *catalog, SV *schema, SV *table, SV *field, SV *attr)
{
  /* all these non-op settings are to stifle OS X compile warnings */
  imp_dbh= imp_dbh;
  dbh= dbh;
  catalog= catalog;
  schema= schema;
  table= table;
  field= field;
  attr= attr;
  return sv_2mortal(my_ulonglong2str(drizzle_insert_id(imp_dbh->pdrizzle)));
}
#endif


int parse_number(char *string, STRLEN len, char **end)
{
    int seen_neg;
    int seen_dec;
    char *cp;

    seen_neg= 0;
    seen_dec= 0;

    if (len <= 0) {
        len= strlen(string);
    }

    cp= string;

    /* Skip leading whitespace */
    while (*cp && isspace(*cp))
        cp++;

    for ( ; *cp; cp++)
    {
        if ('-' == *cp)
        {
            if (seen_neg)
            {
              /* second '-' */
              break;
            }
            else if (cp > string)
            {
              /* '-' after digit(s) */
              break;
            }
            seen_neg= 1;
        }
        else if ('.' == *cp)
        {
            if (seen_dec)
            {
                /* second '.' */
                break;
            }
            seen_dec= 1;
        }
        else if (!isdigit(*cp))
        {
            break;
        }
    }

    *end= cp;

    if (cp - string < (int) len) {
        return -1;
    }

    return 0;
}
