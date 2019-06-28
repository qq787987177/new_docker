/* -*- Mode: C; tab-width: 4 -*- */
/*
  +----------------------------------------------------------------------+
  | PHP Version 5                                                        |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2009 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Original Author: Michael Grunder <michael.grunder@gmail.com          |
  | Maintainer: Nicolas Favre-Felix <n.favre-felix@owlient.eu>           |
  | PHP7 Porter: Xinchen Hui <laruence@php.net>                          |
  +----------------------------------------------------------------------+
*/

#include "redis_commands.h"
#include <zend_smart_str.h>
#include <zend_exceptions.h>

/* Generic commands based on method signature and what kind of things we're
 * processing.  Lots of Redis commands take something like key, value, or
 * key, value long.  Each unique signature like this is written only once */

/* A command that takes no arguments */
int redis_empty_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
    *cmd_len = redis_cmd_format_static(cmd, kw, "");
    return SUCCESS;
}

/* Generic command where we just take a string and do nothing to it*/
int redis_str_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock, char *kw,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
	zend_string *arg;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &arg) == FAILURE) {
        return FAILURE;
    }

    // Build the command without molesting the string
    *cmd_len = redis_cmd_format_static(cmd, kw, "s", ZSTR_VAL(arg), ZSTR_LEN(arg));

    return SUCCESS;
}

/* Key, long, zval (serialized) */
int redis_key_long_val_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
		void **ctx)
{
    zend_string *key, *val;
    zend_long expire;
    zval *z_val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Slz", &key, &expire, &z_val) == FAILURE) {
        return FAILURE;
    }

    // Serialize value, prefix key
    val = redis_serialize(redis_sock, z_val);
    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, kw, "sls",
			ZSTR_VAL(key), ZSTR_LEN(key), expire, ZSTR_VAL(val), ZSTR_LEN(val));

    CMD_SET_SLOT(slot, key);

	zend_string_release(val);
	zend_string_release(key);

    return SUCCESS;
}

/* Generic key, long, string (unserialized) */
int redis_key_long_str_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
		void **ctx)
{
    zend_string *key, *val;
    zend_long lval;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SlS", &key, &lval, &val) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, kw, "sds",
			ZSTR_VAL(key), ZSTR_LEN(key), (int)lval, ZSTR_VAL(val), ZSTR_LEN(val));

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* Generic command construction when we just take a key and value */
int redis_kv_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
		void **ctx)
{
    zend_string *key, *val;
    zval *z_val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sz", &key, &z_val) == FAILURE) {
        return FAILURE;
    }

    val = redis_serialize(redis_sock, z_val);
    key = redis_key_prefix(redis_sock, key);

    // Construct our command
    *cmd_len = redis_cmd_format_static(cmd, kw, "ss", ZSTR_VAL(key), ZSTR_LEN(key), ZSTR_VAL(val), ZSTR_LEN(val));

    CMD_SET_SLOT(slot, key);

	zend_string_release(val);
	zend_string_release(key);

    return SUCCESS;
}

/* Generic command that takes a key and an unserialized value */
int redis_key_str_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
		void **ctx)
{
	zend_string *key, *val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SS", &key, &val) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, kw, "ss", ZSTR_VAL(key), ZSTR_LEN(key), ZSTR_VAL(val), ZSTR_LEN(val));

    CMD_SET_SLOT(slot, key);

    return SUCCESS;
}

/* Key, string, string without serialization (ZCOUNT, ZREMRANGEBYSCORE) */
int redis_key_str_str_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
		void **ctx)
{
    zend_string *key, *val1, *val2;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SSS", &key, &val1, &val2) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, kw, "SSS", key, val1, val2);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* Generic command that takes two keys */
int redis_key_key_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
		void **ctx)
{
    zend_string *key1, *key2;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SS", &key1, &key2) == FAILURE) {
        return FAILURE;
    }

    key1 = redis_key_prefix(redis_sock, key1);
    key2 = redis_key_prefix(redis_sock, key2);

    // If a slot is requested, we can test that they hash the same
    if (slot) {
        // Slots where these keys resolve
        short slot1 = cluster_hash_key(ZSTR_VAL(key1), ZSTR_LEN(key1));
        short slot2 = cluster_hash_key(ZSTR_VAL(key2), ZSTR_LEN(key2));

        // Check if Redis would give us a CROSSLOT error
        if (slot1 != slot2) {
            php_error_docref(NULL, E_WARNING, "Keys don't hash to the same slot");
			zend_string_release(key1);
			zend_string_release(key2);
            return FAILURE;
        }

        // They're both the same
        *slot = slot1;
    }

    *cmd_len = redis_cmd_format_static(cmd, kw, "SS", key1, key2);

    return SUCCESS;
}

/* Generic command construction where we take a key and a long */
int redis_key_long_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
    zend_string *key;
    zend_long lval;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sl", &key, &lval) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    // Disallow zero length keys (for now)
    if (ZSTR_LEN(key) == 0) {
		zend_string_release(key);
        return FAILURE;
    }

    *cmd_len = redis_cmd_format_static(cmd, kw, "Sl", key, lval);

    CMD_SET_SLOT(slot, key);

    return SUCCESS;
}

/* key, long, long */
int redis_key_long_long_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
    zend_string *key;
    zend_long val1, val2;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sll", &key, &val1, &val2) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, kw, "Sll", key, val1, val2);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* Generic command where we take a single key */
int redis_key_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
	zend_string *key;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &key) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, kw, "S", key);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* Generic command where we take a key and a double */
int redis_key_dbl_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
	zend_string *key;
    double val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sd", &key, &val) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    // Construct our command
    *cmd_len = redis_cmd_format_static(cmd, kw, "Sf", key, val);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* Generic to construct SCAN and variant commands */
int redis_fmt_scan_cmd(char **cmd, REDIS_SCAN_TYPE type, char *key, int key_len,
        long it, char *pat, int pat_len, long count)
{
    static char *kw[] = {"SCAN","SSCAN","HSCAN","ZSCAN"};
    int argc;
    smart_str cmdstr = {0};

    // Figure out our argument count
    argc = 1 + (type != TYPE_SCAN) + (pat_len > 0? 2 : 0) + (count > 0? 2 : 0);

    redis_cmd_init_sstr(&cmdstr, argc, kw[type], strlen(kw[type]));

    // Append our key if it's not a regular SCAN command
    if (type != TYPE_SCAN) {
        redis_cmd_append_sstr(&cmdstr, key, key_len);
    }

    // Append cursor
    redis_cmd_append_sstr_long(&cmdstr, it);

    // Append count if we've got one
    if (count) {
        redis_cmd_append_sstr(&cmdstr, "COUNT", sizeof("COUNT") - 1);
        redis_cmd_append_sstr_long(&cmdstr, count);
    }

    // Append pattern if we've got one
    if (pat_len) {
        redis_cmd_append_sstr(&cmdstr, "MATCH", sizeof("MATCH") - 1);
        redis_cmd_append_sstr(&cmdstr, pat, pat_len);
    }

    // Push command to the caller, return length
    *cmd = ZSTR_VAL(cmdstr.s);
    return ZSTR_LEN(cmdstr.s);
}

/* ZRANGE/ZREVRANGE */
int redis_zrange_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, int *withscores,
        short *slot, void **ctx)
{
	zend_string *key;
    zend_long start, end;
    zend_bool ws = 0;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sll|b", &key, &start, &end, &ws) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    if (ws) {
        *cmd_len = redis_cmd_format_static(cmd, kw, "Sdds", key, start, end, "WITHSCORES", sizeof("WITHSCORES")-1);
    } else {
        *cmd_len = redis_cmd_format_static(cmd, kw, "Sdd", key, start, end);
    }

    CMD_SET_SLOT(slot, key);

    // Free key, push out WITHSCORES option
	zend_string_release(key);

    *withscores = ws;

    return SUCCESS;
}

/* ZRANGEBYSCORE/ZREVRANGEBYSCORE */
int redis_zrangebyscore_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, int *withscores,
        short *slot, void **ctx)
{
	zend_string  *key, *start, *end;
    zval *z_opt = NULL, *z_ele;
    int has_limit = 0;
    long limit_low, limit_high;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SSS|a", &key, &start, &end, &z_opt) == FAILURE) {
        return FAILURE;
    }

    // Check for an options array
    if (z_opt && Z_TYPE_P(z_opt) == IS_ARRAY) {
		HashTable *ht_opt = Z_ARRVAL_P(z_opt);

        // Check for WITHSCORES
        *withscores = ((z_ele = zend_hash_str_find(ht_opt, "withscores", sizeof("withscores") - 1)) != NULL
                && Z_TYPE_P(z_ele) == IS_TRUE);

        // LIMIT
        if ((z_ele = zend_hash_str_find(ht_opt,"limit", sizeof("limit") - 1)) != NULL) {
            HashTable *ht_limit = Z_ARRVAL_P(z_ele);
            zval *z_off, *z_cnt;
            if ((z_cnt = zend_hash_index_find(ht_limit,0)) != NULL &&
                    (z_off = zend_hash_index_find(ht_limit,1)) != NULL &&
                    Z_TYPE_P(z_off)==IS_LONG && Z_TYPE_P(z_cnt) == IS_LONG)
            {
                has_limit  = 1;
                limit_low  = Z_LVAL_P(z_off);
                limit_high = Z_LVAL_P(z_cnt);
            }
        }
    }

    key = redis_key_prefix(redis_sock, key);
    CMD_SET_SLOT(slot, key);

    if (*withscores) {
        if (has_limit) {
            *cmd_len = redis_cmd_format_static(cmd, kw, "SSSsdds",
					key, start, end, "LIMIT", 5, limit_low, limit_high, "WITHSCORES", 10);
        } else {
            *cmd_len = redis_cmd_format_static(cmd, kw, "SSSs", key, start, end, "WITHSCORES", 10);
        }
    } else {
        if (has_limit) {
            *cmd_len = redis_cmd_format_static(cmd, kw, "SSSsdd",
					key, start, end, "LIMIT", 5, limit_low, limit_high);
        } else {
            *cmd_len = redis_cmd_format_static(cmd, kw, "SSS", key, start, end);
        }
    }

	zend_string_release(key);

    return SUCCESS;
}

/* ZUNIONSTORE, ZINTERSTORE */
int redis_zinter_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
	zend_string *key, *agg_op = NULL;
    zval *z_keys, *z_weights=NULL, *z_ele;
    HashTable *ht_keys, *ht_weights=NULL;
    smart_str cmdstr = {0};
    int argc = 2, keys_count;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sa|a!S", &key, &z_keys, &z_weights, &agg_op) == FAILURE) {
        return FAILURE;
    }

    // Grab our keys
    ht_keys = Z_ARRVAL_P(z_keys);

    // Nothing to do if there aren't any
    if ((keys_count = zend_hash_num_elements(ht_keys)) == 0) {
        return FAILURE;
    } else {
        argc += keys_count;
    }

    // Handle WEIGHTS
    if (z_weights != NULL) {
        ht_weights = Z_ARRVAL_P(z_weights);
        if (zend_hash_num_elements(ht_weights) != keys_count) {
            php_error_docref(NULL, E_WARNING,
                    "WEIGHTS and keys array should be the same size!");
            return FAILURE;
        }

        // "WEIGHTS" + key count
        argc += keys_count + 1;
    }

    // AGGREGATE option
    if (agg_op && ZSTR_LEN(agg_op)) {
        if (strncasecmp(ZSTR_VAL(agg_op), "SUM", sizeof("SUM")) &&
                strncasecmp(ZSTR_VAL(agg_op), "MIN", sizeof("MIN")) &&
                strncasecmp(ZSTR_VAL(agg_op), "MAX", sizeof("MAX")))
        {
            php_error_docref(NULL, E_WARNING,
                    "Invalid AGGREGATE option provided!");
            return FAILURE;
        }

        // "AGGREGATE" + type
        argc += 2;
    }

    key = redis_key_prefix(redis_sock, key);

    // Start building our command
    redis_cmd_init_sstr(&cmdstr, argc, kw, strlen(kw));
    redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));
    redis_cmd_append_sstr_int(&cmdstr, keys_count);

    CMD_SET_SLOT(slot, key);
	zend_string_release(key);

    // Process input keys
    ZEND_HASH_FOREACH_VAL(ht_keys, z_ele) {
		zend_string *zkey = zval_get_string(z_ele);

        key = redis_key_prefix(redis_sock, zkey);

        // If we're in Cluster mode, verify the slot is the same
        if (slot && *slot != cluster_hash_key(ZSTR_VAL(key), ZSTR_LEN(key))) {
            php_error_docref(NULL, E_WARNING, "All keys don't hash to the same slot!");
			smart_str_free(&cmdstr);
			zend_string_release(key);
			zend_string_release(zkey);
            return FAILURE;
        }

        // Append this input set
        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));

		zend_string_release(key);
		zend_string_release(zkey);
    } ZEND_HASH_FOREACH_END();

    // Weights
    if (ht_weights != NULL) {
        redis_cmd_append_sstr(&cmdstr, "WEIGHTS", sizeof("WEIGHTS")-1);

        // Process our weights
        ZEND_HASH_FOREACH_VAL(ht_weights, z_ele) {
            // Ignore non numeric args unless they're inf/-inf
            if (Z_TYPE_P(z_ele) != IS_LONG &&
				Z_TYPE_P(z_ele) != IS_DOUBLE &&
					(Z_TYPE_P(z_ele) != IS_STRING ||
					 (strncasecmp(Z_STRVAL_P(z_ele),"inf",sizeof("inf")) != 0 &&
					  strncasecmp(Z_STRVAL_P(z_ele),"-inf",sizeof("-inf")) != 0 &&
					  strncasecmp(Z_STRVAL_P(z_ele),"+inf",sizeof("+inf")) != 0))) {
                php_error_docref(NULL, E_WARNING, "Weights must be numeric or '-inf','inf','+inf'");
				smart_str_free(&cmdstr);
                return FAILURE;
            }

            switch (Z_TYPE_P(z_ele)) {
                case IS_LONG:
                    redis_cmd_append_sstr_long(&cmdstr, Z_LVAL_P(z_ele));
                    break;
                case IS_DOUBLE:
                    redis_cmd_append_sstr_dbl(&cmdstr, Z_DVAL_P(z_ele));
                    break;
                case IS_STRING:
                    redis_cmd_append_sstr(&cmdstr, Z_STRVAL_P(z_ele), Z_STRLEN_P(z_ele));
                    break;
            }
        } ZEND_HASH_FOREACH_END();
    }

    // AGGREGATE
    if (agg_op && ZSTR_LEN(agg_op) != 0) {
        redis_cmd_append_sstr(&cmdstr, "AGGREGATE", sizeof("AGGREGATE")-1);
        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(agg_op), ZSTR_LEN(agg_op));
    }

    // Push out values
    *cmd     = ZSTR_VAL(cmdstr.s);
    *cmd_len = ZSTR_LEN(cmdstr.s);

    return SUCCESS;
}

/* SUBSCRIBE/PSUBSCRIBE */
int redis_subscribe_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
    zval *z_arr, *z_chan;
    HashTable *ht_chan;
    smart_str cmdstr = {0};
	subscribeContext *sctx = emalloc(sizeof(subscribeContext));
	zend_string *key;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "af", &z_arr, &(sctx->cb), &(sctx->cb_cache)) == FAILURE) {
        efree(sctx);
        return FAILURE;
    }

    ht_chan    = Z_ARRVAL_P(z_arr);
    sctx->kw   = kw;
    sctx->argc = zend_hash_num_elements(ht_chan);

    if (sctx->argc==0) {
        efree(sctx);
        return FAILURE;
    }

    // Start command construction
    redis_cmd_init_sstr(&cmdstr, sctx->argc, kw, strlen(kw));

    // Iterate over channels
    ZEND_HASH_FOREACH_VAL(ht_chan, z_chan) {
		zend_string *zkey = zval_get_string(z_chan);

        key = redis_key_prefix(redis_sock, zkey);

        // Add this channel
        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));

		zend_string_release(key);
		zend_string_release(zkey);
    } ZEND_HASH_FOREACH_END();

    // Push values out
    *cmd_len = ZSTR_LEN(cmdstr.s);
    *cmd     = ZSTR_VAL(cmdstr.s);
    *ctx     = (void*)sctx;

    // Pick a slot at random
    CMD_RAND_SLOT(slot);

    return SUCCESS;
}

/* UNSUBSCRIBE/PUNSUBSCRIBE */
int redis_unsubscribe_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
    zval *z_arr, *z_chan;
    HashTable *ht_arr;
    smart_str cmdstr = {0};
    subscribeContext *sctx = emalloc(sizeof(subscribeContext));

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "a", &z_arr) == FAILURE) {
        efree(sctx);
        return FAILURE;
    }

    ht_arr = Z_ARRVAL_P(z_arr);

    sctx->argc = zend_hash_num_elements(ht_arr);
    if (sctx->argc == 0) {
        efree(sctx);
        return FAILURE;
    }

    redis_cmd_init_sstr(&cmdstr, sctx->argc, kw, strlen(kw));

    ZEND_HASH_FOREACH_VAL(ht_arr, z_chan) {
        zend_string *key;
		zend_string *zkey = zval_get_string(z_chan);

        key = redis_key_prefix(redis_sock, zkey);
        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));

		zend_string_release(key);
		zend_string_release(zkey);
    } ZEND_HASH_FOREACH_END();

    // Push out vals
    *cmd_len = ZSTR_LEN(cmdstr.s);
    *cmd     = ZSTR_VAL(cmdstr.s);
    *ctx     = (void*)sctx;

    return SUCCESS;
}

/* ZRANGEBYLEX/ZREVRANGEBYLEX */
int redis_zrangebylex_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
    zend_string *key, *min, *max;
    zend_long offset, count;
    int argc = ZEND_NUM_ARGS();

    /* We need either 3 or 5 arguments for this to be valid */
    if (argc != 3 && argc != 5) {
        php_error_docref(0, E_WARNING, "Must pass either 3 or 5 arguments");
        return FAILURE;
    }

    if (zend_parse_parameters(argc, "SSS|ll", &key, &min, &max, &offset, &count) == FAILURE) {
        return FAILURE;
    }

    /* min and max must start with '(' or '[' */
    if (ZSTR_LEN(min) < 1 || ZSTR_LEN(max) < 1 || (ZSTR_VAL(min)[0] != '(' && ZSTR_VAL(min)[0] != '[') ||
            (ZSTR_VAL(max)[0] != '(' && ZSTR_VAL(max)[0] != '[')) {
        php_error_docref(0, E_WARNING, "min and max arguments must start with '[' or '('");
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    /* Construct command */
    if (argc == 3) {
        *cmd_len = redis_cmd_format_static(cmd, kw, "SSS", key, min, max);
    } else {
        *cmd_len = redis_cmd_format_static(cmd, kw, "SSSsll",
				key, min, max, "LIMIT", sizeof("LIMIT")-1, offset, count);
    }

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* ZLEXCOUNT/ZREMRANGEBYLEX */
int redis_gen_zlex_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot, 
        void **ctx)
{
    zend_string *key, *min, *max;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SSS", &key, &min, &max) == FAILURE) {
        return FAILURE;
    }

    /* Quick sanity check on min/max */
    if (ZSTR_LEN(min) < 1 || ZSTR_LEN(max) < 1 ||
			(ZSTR_VAL(min)[0] != '(' && ZSTR_VAL(min)[0] != '[') ||
            (ZSTR_VAL(max)[0] != '(' && ZSTR_VAL(max)[0] != '['))
    {
        php_error_docref(NULL, E_WARNING, "Min and Max arguments must begin with '(' or '['");
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, kw, "SSS", key, min, max);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* Commands that take a key followed by a variable list of serializable
 * values (RPUSH, LPUSH, SADD, SREM, etc...) */
int redis_key_varval_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
    zval *z_args;
    smart_str cmdstr = {0};
    zend_string *arg;
    zend_string *tmp_arg;
    int i, argc = ZEND_NUM_ARGS();

    // We at least need a key and one value
    if (argc < 2) {
        return FAILURE;
    }

    // Make sure we at least have a key, and we can get other args
    z_args = safe_emalloc(sizeof(zval), argc, 0);
    if (zend_get_parameters_array(ht, argc, z_args) == FAILURE) {
        efree(z_args);
        return FAILURE;
    }

    // Grab the first argument (our key) as a string
    tmp_arg = zval_get_string(&z_args[0]);

    arg = redis_key_prefix(redis_sock, tmp_arg);

    // Start command construction
    redis_cmd_init_sstr(&cmdstr, argc, kw, strlen(kw));
    redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(arg), ZSTR_LEN(arg));

    // Set our slot, free key prefix if we prefixed it
    CMD_SET_SLOT(slot, arg);
	zend_string_release(tmp_arg);
	zend_string_release(arg);

    // Add our members
    for(i = 1; i < argc; i++) {
        arg = redis_serialize(redis_sock, &z_args[i]);
        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(arg), ZSTR_LEN(arg));
		zend_string_release(arg);
    }

    // Push out values
    *cmd     = ZSTR_VAL(cmdstr.s);
    *cmd_len = ZSTR_LEN(cmdstr.s);

    // Cleanup arg array
    efree(z_args);

    return SUCCESS;
}

/* Generic function that takes a variable number of keys, with an optional
 * timeout value.  This can handle various SUNION/SUNIONSTORE/BRPOP type
 * commands. */
static int gen_varkey_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, int kw_len, int min_argc, int has_timeout,
        char **cmd, int *cmd_len, short *slot)
{
    zval *z_args, *z_ele;
    HashTable *ht_arr;
    zend_string *key;
    int i, tail;
    int single_array = 0, argc = ZEND_NUM_ARGS();
    smart_str cmdstr = {0};
    long timeout;
    short kslot = -1;

    if (argc < min_argc) {
        zend_wrong_param_count();
        return FAILURE;
    }

    // Allocate args
    z_args = safe_emalloc(sizeof(zval), argc, 0);
    if (zend_get_parameters_array(ht, argc, z_args) == FAILURE) {
        efree(z_args);
        return FAILURE;
    }

    // Handle our "single array" case
    if (has_timeout == 0) {
        single_array = argc == 1 && Z_TYPE(z_args[0]) == IS_ARRAY;
    } else {
        single_array = argc == 2 && Z_TYPE(z_args[0]) ==IS_ARRAY && Z_TYPE(z_args[1]) == IS_LONG;
        timeout = Z_LVAL(z_args[1]);
    }

    // If we're running a single array, rework args
    if (single_array) {
        ht_arr = Z_ARRVAL(z_args[0]);
        argc = zend_hash_num_elements(ht_arr);
        if (has_timeout) {
			argc++;
		}
        efree(z_args);
        z_args = NULL;

        /* If the array is empty, we can simply abort */
        if (argc == 0) {
			return FAILURE;
		}
    }

    // Begin construction of our command
    redis_cmd_init_sstr(&cmdstr, argc, kw, kw_len);

    if (single_array) {
        ZEND_HASH_FOREACH_VAL(ht_arr, z_ele) {
			zend_string *zkey = zval_get_string(z_ele);
            key = redis_key_prefix(redis_sock, zkey);

            // Protect against CROSSLOT errors
            if (slot) {
                if (kslot == -1) {
                    kslot = cluster_hash_key(ZSTR_VAL(key), ZSTR_LEN(key));
                } else if (cluster_hash_key(ZSTR_VAL(key), ZSTR_LEN(key)) != kslot) {
                    php_error_docref(NULL, E_WARNING, "Not all keys hash to the same slot!");
                    return FAILURE;
                }
            }

            // Append this key, free it if we prefixed
            redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));

			zend_string_release(key);
			zend_string_release(zkey);
        } ZEND_HASH_FOREACH_END();

        if (has_timeout) {
            redis_cmd_append_sstr_long(&cmdstr, timeout);
        }
    } else {
        if (has_timeout && Z_TYPE(z_args[argc-1]) != IS_LONG) {
            php_error_docref(NULL, E_ERROR, "Timeout value must be a LONG");
            efree(z_args);
            return FAILURE;
        }

        tail = has_timeout ? argc-1 : argc;
        for(i = 0; i < tail; i++) {
			zend_string *zkey = zval_get_string(&z_args[i]);

            key = redis_key_prefix(redis_sock, zkey);

            /* Protect against CROSSSLOT errors if we've got a slot */
            if (slot) {
                if (kslot == -1) {
                    kslot = cluster_hash_key(ZSTR_VAL(key), ZSTR_LEN(key));
                } else if (cluster_hash_key(ZSTR_VAL(key), ZSTR_LEN(key)) != kslot) {
                    php_error_docref(NULL, E_WARNING, "Not all keys hash to the same slot");
                    efree(z_args);
                    return FAILURE;
                }
            }

            redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));
			zend_string_release(key);
			zend_string_release(zkey);
        }

        if (has_timeout) {
            redis_cmd_append_sstr_long(&cmdstr, Z_LVAL(z_args[tail]));
        }

        // Cleanup args
        efree(z_args);
    }

    // Push out parameters
    if (slot) {
		*slot = kslot;
	}

    *cmd = ZSTR_VAL(cmdstr.s);
    *cmd_len = ZSTR_LEN(cmdstr.s);

    return SUCCESS;
}

/*
 * Commands with specific signatures or that need unique functions because they
 * have specific processing (argument validation, etc) that make them unique
 */

/* SET */
int redis_set_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zval *z_value, *z_opts=NULL;
    char *exp_type = NULL, *set_type = NULL;
    zend_string *key, *val;
    long expire = -1;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sz|z", &key, &z_value, &z_opts) == FAILURE) {
        return FAILURE;
    }

    /* Our optional argument can either be a long (to support legacy SETEX */
    /* redirection), or an array with Redis >= 2.6.12 set options */
    if (z_opts && Z_TYPE_P(z_opts) != IS_LONG && Z_TYPE_P(z_opts) != IS_ARRAY && Z_TYPE_P(z_opts) != IS_NULL) {
        return FAILURE;
    }

    val = redis_serialize(redis_sock, z_value);
    key = redis_key_prefix(redis_sock, key);

    // Check for an options array
    if (z_opts && Z_TYPE_P(z_opts) == IS_ARRAY) {
        HashTable *kt = Z_ARRVAL_P(z_opts);
        zend_string *k;
        zval *v;

        /* Iterate our option array */
        ZEND_HASH_FOREACH_STR_KEY_VAL(kt, k, v) {
            if (k && (Z_TYPE_P(v) == IS_LONG) && (Z_LVAL_P(v) > 0) && IS_EX_PX_ARG(ZSTR_VAL(k))) {
                exp_type = ZSTR_VAL(k);
                expire = Z_LVAL_P(v);
            } else if (Z_TYPE_P(v) == IS_STRING && IS_NX_XX_ARG(Z_STRVAL_P(v))) {
                set_type = Z_STRVAL_P(v);
            }
        } ZEND_HASH_FOREACH_END();
    } else if (z_opts && Z_TYPE_P(z_opts) == IS_LONG) {
        expire = Z_LVAL_P(z_opts);
    }

    /* Now let's construct the command we want */
    if (exp_type && set_type) {
        /* SET <key> <value> NX|XX PX|EX <timeout> */
        *cmd_len = redis_cmd_format_static(cmd, "SET", "SSssl", key, val, set_type, 2, exp_type, 2, expire);
    } else if (exp_type) {
        /* SET <key> <value> PX|EX <timeout> */
        *cmd_len = redis_cmd_format_static(cmd, "SET", "SSsl", key, val, exp_type, 2, expire);
    } else if (set_type) {
        /* SET <key> <value> NX|XX */
        *cmd_len = redis_cmd_format_static(cmd, "SET", "SSs", key, val, set_type, 2);
    } else if (expire > 0) {
        /* Backward compatible SETEX redirection */
        *cmd_len = redis_cmd_format_static(cmd, "SETEX", "SlS", key, expire, val);
    } else {
        /* SET <key> <value> */
        *cmd_len = redis_cmd_format_static(cmd, "SET", "SS", key, val);
    }

    // If we've been passed a slot pointer, return the key's slot
    CMD_SET_SLOT(slot, key);

    zend_string_release(key);
    zend_string_release(val);

    return SUCCESS;
}

/* BRPOPLPUSH */
int redis_brpoplpush_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zend_string *key1, *key2;
    short slot1, slot2;
    zend_long timeout;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SSl", &key1, &key2, &timeout) == FAILURE) {
        return FAILURE;
    }

    key1 = redis_key_prefix(redis_sock, key1);
    key2 = redis_key_prefix(redis_sock, key2);

    // In cluster mode, verify the slots match
    if (slot) {
        slot1 = cluster_hash_key(ZSTR_VAL(key1), ZSTR_LEN(key1));
        slot2 = cluster_hash_key(ZSTR_VAL(key2), ZSTR_LEN(key2));
        if (slot1 != slot2) {
            php_error_docref(NULL, E_WARNING, "Keys hash to different slots!");
			zend_string_release(key1);
			zend_string_release(key2);
            return FAILURE;
        }

        // Both slots are the same
        *slot = slot1;
    }

    // Consistency with Redis, if timeout < 0 use RPOPLPUSH
    if (timeout < 0) {
        *cmd_len = redis_cmd_format_static(cmd, "RPOPLPUSH", "SS", key1, key2);
    } else {
        *cmd_len = redis_cmd_format_static(cmd, "BRPOPLPUSH", "SSd", key1, key2, timeout);
    }

    return SUCCESS;
}

/* To maintain backward compatibility with earlier versions of phpredis, we 
 * allow for an optional "increment by" argument for INCR and DECR even though
 * that's not how Redis proper works */
#define TYPE_INCR 0
#define TYPE_DECR 1

/* Handle INCR(BY) and DECR(BY) depending on optional increment value */
static int 
redis_atomic_increment(INTERNAL_FUNCTION_PARAMETERS, int type, 
                       RedisSock *redis_sock, char **cmd, int *cmd_len, 
                       short *slot, void **ctx)
{
    zend_string *key;
    zend_long val = 1;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "S|l", &key, &val) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    /* If our value is 1 we use INCR/DECR.  For other values, treat the call as
     * an INCRBY or DECRBY call */
    if (type == TYPE_INCR) {
        if (val == 1) {
           *cmd_len = redis_cmd_format_static(cmd, "INCR", "S", key);
        } else {
           *cmd_len = redis_cmd_format_static(cmd, "INCRBY", "Sd", key, val);
        }
    } else {
        if (val == 1) {
            *cmd_len = redis_cmd_format_static(cmd, "DECR", "S", key);
        } else {
            *cmd_len = redis_cmd_format_static(cmd,"DECRBY", "Sd", key, val);
        }
    }

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}
 
/* INCR */
int redis_incr_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
                   char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return redis_atomic_increment(INTERNAL_FUNCTION_PARAM_PASSTHRU,
        TYPE_INCR, redis_sock, cmd, cmd_len, slot, ctx);
}

/* DECR */
int redis_decr_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
                   char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return redis_atomic_increment(INTERNAL_FUNCTION_PARAM_PASSTHRU,
        TYPE_DECR, redis_sock, cmd, cmd_len, slot, ctx);
}

/* HINCRBY */
int redis_hincrby_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
	zend_string *key, *mem;
    zend_long byval;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SSl", &key, &mem, &byval) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, "HINCRBY", "SSd", key, mem, byval);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* HINCRBYFLOAT */
int redis_hincrbyfloat_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zend_string *key, *mem;
    double byval;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SSd", &key, &mem, &byval) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, "HINCRBYFLOAT", "SSf", key, mem, byval);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* HMGET */
int redis_hmget_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zend_string *key;
    zval *z_arr, *z_mems, *z_mem;
    int i, count, valid = 0;
    HashTable *ht_arr;
    smart_str cmdstr = {0};

    // Parse arguments
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sa", &key, &z_arr) == FAILURE) {
        return FAILURE;
    }

    ht_arr = Z_ARRVAL_P(z_arr);

    // We can abort if we have no elements
    if ((count = zend_hash_num_elements(ht_arr)) == 0) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    // Allocate memory for mems+1 so we can have a sentinel
    z_mems = ecalloc(count + 1, sizeof(zval));

    // Iterate over our member array
    ZEND_HASH_FOREACH_VAL(ht_arr, z_mem) {
        // We can only handle string or long values here
        if ((Z_TYPE_P(z_mem) == IS_STRING && Z_STRLEN_P(z_mem)) || Z_TYPE_P(z_mem) == IS_LONG) {
            ZVAL_NEW_STR(&z_mems[valid], zval_get_string(z_mem));
            // Increment the member count to actually send
            valid++;
        }
    } ZEND_HASH_FOREACH_END();

    // If nothing was valid, fail
    if (valid == 0) {
		zend_string_release(key);
        efree(z_mems);
        return FAILURE;
    }

    // Sentinel so we can free this even if it's used and then we discard
    // the transaction manually or there is a transaction failure
    ZVAL_UNDEF(&z_mems[valid]);

    // Start command construction
    redis_cmd_init_sstr(&cmdstr, valid + 1, "HMGET", sizeof("HMGET")-1);
    redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));

    // Iterate over members, appending as arguments
    for(i = 0; i < valid; i++) {
        redis_cmd_append_sstr(&cmdstr, Z_STRVAL(z_mems[i]), Z_STRLEN(z_mems[i]));
    }

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    // Push out command, length, and key context
    *cmd     = ZSTR_VAL(cmdstr.s);
    *cmd_len = ZSTR_LEN(cmdstr.s);
    *ctx     = (void*)z_mems;

    return SUCCESS;
}

/* HMSET */
int redis_hmset_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
	zend_string *key;
    zend_string *mem;
    int count;
    zend_ulong idx;
    zval *z_arr;
    HashTable *ht_vals;
    smart_str cmdstr = {0};
    zval *z_val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sa", &key, &z_arr) == FAILURE) {
        return FAILURE;
    }

    // We can abort if we have no fields
    if ((count = zend_hash_num_elements(Z_ARRVAL_P(z_arr))) == 0) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    // Grab our array as a HashTable
    ht_vals = Z_ARRVAL_P(z_arr);

    // Initialize our HMSET command (key + 2x each array entry), add key
    redis_cmd_init_sstr(&cmdstr, 1 + (count*2), "HMSET", sizeof("HMSET")-1);
    redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));

    // Start traversing our key => value array
    ZEND_HASH_FOREACH_KEY_VAL(ht_vals, idx, mem, z_val) {
        zend_string *val;

        // If the hash key is an integer, convert it to a string
        if (mem) {
			zend_string_copy(mem);
        } else {
            mem = strpprintf(40/*sizeof(kbuf)*/, "%ld", (long)idx);
        }

        // Serialize value (if directed)
        val = redis_serialize(redis_sock, z_val);

        // Append the key and value to our command
        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(mem), ZSTR_LEN(mem));
        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(val), ZSTR_LEN(val));

		zend_string_release(mem);
		zend_string_release(val);
    } ZEND_HASH_FOREACH_END();

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    // Push return pointers
    *cmd_len = ZSTR_LEN(cmdstr.s);
    *cmd = ZSTR_VAL(cmdstr.s);

    // Success!
    return SUCCESS;
}

/* BITPOS */
int redis_bitpos_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zend_string *key;
    zend_long bit, start, end;
    int argc;

    argc = ZEND_NUM_ARGS();
    if (zend_parse_parameters(argc, "Sl|ll", &key, &bit, &start, &end) == FAILURE) {
        return FAILURE;
    }

    // Prevalidate bit
    if (bit != 0 && bit != 1) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    if (argc == 2) {
        *cmd_len = redis_cmd_format_static(cmd, "BITPOS", "Sd", key, bit);
    } else if (argc == 3) {
        *cmd_len = redis_cmd_format_static(cmd, "BITPOS", "Sdd", key, bit, start);
    } else {
        *cmd_len = redis_cmd_format_static(cmd, "BITPOS", "Sddd", key, bit, start, end);
    }

    CMD_SET_SLOT(slot, key);

    return SUCCESS;
}

/* BITOP */
int redis_bitop_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zval *z_args;
    zend_string *key;
    int i, argc = ZEND_NUM_ARGS();
    smart_str cmdstr = {0};
    short kslot;

    // Allocate space for args, parse them as an array
    z_args = safe_emalloc(sizeof(zval), argc, 0);
    if (zend_get_parameters_array(ht, argc, z_args) == FAILURE || argc < 3 || Z_TYPE(z_args[0]) != IS_STRING) {
        efree(z_args);
        return FAILURE;
    }

    // If we were passed a slot pointer, init to a sentinel value
    if (slot) { 
		*slot = -1;
	}

    // Initialize command construction, add our operation argument
    redis_cmd_init_sstr(&cmdstr, argc, "BITOP", sizeof("BITOP")-1);
    redis_cmd_append_sstr(&cmdstr, Z_STRVAL(z_args[0]), Z_STRLEN(z_args[0]));

    // Now iterate over our keys argument
    for(i = 1; i < argc; i++) {
		zend_string *zkey = zval_get_string(&z_args[i]);

        key = redis_key_prefix(redis_sock, zkey);

        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));

        if (slot) {
            kslot = cluster_hash_key(ZSTR_VAL(key), ZSTR_LEN(key));
            if (*slot == -1 || kslot != *slot) {
                php_error_docref(NULL, E_WARNING, "Warning, not all keys hash to the same slot!");
				zend_string_release(key);
				zend_string_release(zkey);
                return FAILURE;
            }
            *slot = kslot;
        }

		zend_string_release(key);
		zend_string_release(zkey);
	}

    efree(z_args);

    // Push out variables
    *cmd = ZSTR_VAL(cmdstr.s);
    *cmd_len = ZSTR_LEN(cmdstr.s);

    return SUCCESS;
}

/* BITCOUNT */
int redis_bitcount_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zend_string *key;
    zend_long start = 0, end = -1;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "S|ll", &key, &start, &end) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, "BITCOUNT", "Sdd", key, (int)start, (int)end);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* PFADD and PFMERGE are the same except that in one case we serialize,
 * and in the other case we key prefix */
static int redis_gen_pf_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, int kw_len, int is_keys, char **cmd,
        int *cmd_len, short *slot)
{
    zval *z_arr, *z_ele;
    HashTable *ht_arr;
    smart_str cmdstr = {0};
    zend_string *key, *mem;
    int argc=1;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sa", &key, &z_arr) == FAILURE) {
        return FAILURE;
    }

    // Grab HashTable, count total argc
    ht_arr = Z_ARRVAL_P(z_arr);
    argc += zend_hash_num_elements(ht_arr);

    // We need at least two arguments
    if (argc < 2) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    CMD_SET_SLOT(slot, key);

    // Start command construction
    redis_cmd_init_sstr(&cmdstr, argc, kw, kw_len);
    redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));

	zend_string_release(key);

    // Now iterate over the rest of our keys or values
    ZEND_HASH_FOREACH_VAL(ht_arr, z_ele) {

        if (is_keys) {
			zend_string *tmp_mem = zval_get_string(z_ele);

            mem = redis_key_prefix(redis_sock, tmp_mem);

            // Verify slot
            if (slot && *slot != cluster_hash_key(ZSTR_VAL(mem), ZSTR_LEN(mem))) {
                php_error_docref(0, E_WARNING, "All keys must hash to the same slot!");
				zend_string_release(tmp_mem);
				zend_string_release(key);
				zend_string_release(mem);
                return FAILURE;
            }
			zend_string_release(tmp_mem);
        } else {
            mem = redis_serialize(redis_sock, z_ele);
            if (!mem) {
                mem = zval_get_string(z_ele);
            }
        }

        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(mem), ZSTR_LEN(mem));

		zend_string_release(mem);

    } ZEND_HASH_FOREACH_END();

    // Push output arguments
    *cmd = ZSTR_VAL(cmdstr.s);
    *cmd_len = ZSTR_LEN(cmdstr.s);

    return SUCCESS;
}

/* PFADD */
int redis_pfadd_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return redis_gen_pf_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "PFADD", sizeof("PFADD")-1, 0, cmd, cmd_len, slot);
}

/* PFMERGE */
int redis_pfmerge_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return redis_gen_pf_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "PFMERGE", sizeof("PFMERGE")-1, 1, cmd, cmd_len, slot);
}

/* PFCOUNT */
int redis_pfcount_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
                      char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zval *z_keys, *z_key;
    HashTable *ht_keys;
    smart_str cmdstr = {0};
    int num_keys;
    zend_string *key;
    short kslot = -1;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "z", &z_keys) == FAILURE) {
        return FAILURE;
    }

    /* If we were passed an array of keys, iterate through them prefixing if
     * required and capturing lengths and if we need to free them.  Otherwise
     * attempt to treat the argument as a string and just pass one */
    if (Z_TYPE_P(z_keys) == IS_ARRAY) {
        /* Grab key hash table and the number of keys */
        ht_keys = Z_ARRVAL_P(z_keys);
        num_keys = zend_hash_num_elements(ht_keys);

        /* There is no reason to send zero keys */
        if (num_keys == 0) {
            return FAILURE;
        }

        /* Initialize the command with our number of arguments */
        redis_cmd_init_sstr(&cmdstr, num_keys, "PFCOUNT", sizeof("PFCOUNT")-1);
        
        /* Append our key(s) */
        ZEND_HASH_FOREACH_VAL(ht_keys, z_key) {
			zend_string *zkey = zval_get_string(z_key);

            key = redis_key_prefix(redis_sock, zkey);

            redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));
            
            /* Protect against CROSSLOT errors */
            if (slot) {
                if (kslot == -1) {
                    kslot = cluster_hash_key(ZSTR_VAL(key), ZSTR_LEN(key));
                } else if (cluster_hash_key(ZSTR_VAL(key), ZSTR_LEN(key)) != kslot) {
					zend_string_release(key);
					zend_string_release(zkey);
                    smart_str_free(&cmdstr);
                    
                    php_error_docref(NULL, E_WARNING, "Not all keys hash to the same slot!");
                    return FAILURE;
                }
            }

			zend_string_release(key);
			zend_string_release(zkey);
        } ZEND_HASH_FOREACH_END();
    } else {
		zend_string *zkey = zval_get_string(z_keys);


        /* Construct our whole command */
        redis_cmd_init_sstr(&cmdstr, 1, "PFCOUNT", sizeof("PFCOUNT")-1);
		key = redis_key_prefix(redis_sock, zkey);
        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));

        /* Hash our key */
        CMD_SET_SLOT(slot, key);

		zend_string_release(key);
		zend_string_release(zkey);
    }

    /* Push our command and length to the caller */
    *cmd = ZSTR_VAL(cmdstr.s);
    *cmd_len = ZSTR_LEN(cmdstr.s);

    return SUCCESS;
}

int redis_auth_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
	zend_string *pw;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &pw) == FAILURE) {
        return FAILURE;
    }

    // Construct our AUTH command
    *cmd_len = redis_cmd_format_static(cmd, "AUTH", "S", pw);

    // Free previously allocated password, and update
    if (redis_sock->auth) {
		efree(redis_sock->auth);
	}

    redis_sock->auth = estrndup(ZSTR_VAL(pw), ZSTR_LEN(pw));

    return SUCCESS;
}

/* SETBIT */
int redis_setbit_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zend_string *key;
    zend_long offset;
    zend_bool val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Slb", &key, &offset, &val) == FAILURE) {
        return FAILURE;
    }

    // Validate our offset
    if (offset < BITOP_MIN_OFFSET || offset > BITOP_MAX_OFFSET) {
        php_error_docref(0, E_WARNING, "Invalid OFFSET for bitop command (must be between 0-2^32-1)");
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);
    *cmd_len = redis_cmd_format_static(cmd, "SETBIT", "Sld", key, offset, (int)val);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* LINSERT */
int redis_linsert_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zend_string *key, *pos;
    zval *z_val, *z_pivot;
    zend_string *val, *pivot;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SSzz", &key, &pos, &z_pivot, &z_val) == FAILURE) {
        return FAILURE;
    }

    // Validate position
    if (strncasecmp(ZSTR_VAL(pos), "after", 5) && strncasecmp(ZSTR_VAL(pos), "before", 6)) {
        php_error_docref(NULL, E_WARNING, "Position must be either 'BEFORE' or 'AFTER'");
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);
    val = redis_serialize(redis_sock, z_val);
    pivot = redis_serialize(redis_sock, z_pivot);

    *cmd_len = redis_cmd_format_static(cmd, "LINSERT", "SSSS", key, pos, pivot, val);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);
	zend_string_release(val);
	zend_string_release(pivot);

    return SUCCESS;
}

/* LREM */
int redis_lrem_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zend_string *key;
    zend_long count = 0;
    zval *z_val;
    zend_string *val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sz|l", &key, &z_val, &count) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);
    val = redis_serialize(redis_sock, z_val);

    *cmd_len = redis_cmd_format_static(cmd, "LREM", "SdS", key, count, val);

    CMD_SET_SLOT(slot, key);

	zend_string_release(val);
	zend_string_release(key);

    return SUCCESS;
}

int redis_smove_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zend_string *src, *dst;
    zval *z_val;
    zend_string *val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SSz", &src, &dst, &z_val) == FAILURE) {
        return FAILURE;
    }

    val = redis_serialize(redis_sock, z_val);
    src = redis_key_prefix(redis_sock, src);
    dst = redis_key_prefix(redis_sock, dst);

    // Protect against a CROSSSLOT error
    if (slot) {
        short slot1 = cluster_hash_key(ZSTR_VAL(src), ZSTR_LEN(src));
        short slot2 = cluster_hash_key(ZSTR_VAL(dst), ZSTR_LEN(dst));
        if (slot1 != slot2) {
            php_error_docref(0, E_WARNING, "Source and destination keys don't hash to the same slot!");
			zend_string_release(val);
			zend_string_release(src);
			zend_string_release(dst);
			return FAILURE;
        }
        *slot = slot1;
    }

    *cmd_len = redis_cmd_format_static(cmd, "SMOVE", "SSS", src, dst, val);

    zend_string_release(val);
    zend_string_release(src);
    zend_string_release(dst);

    return SUCCESS;
}

/* Generic command construction for HSET and HSETNX */
static int gen_hset_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char *kw, char **cmd, int *cmd_len, short *slot)
{
    zend_string *key, *val, *mem;
    zval *z_val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SSz", &key, &mem, &z_val) == FAILURE) {
        return FAILURE;
    }

    val = redis_serialize(redis_sock, z_val);
    key = redis_key_prefix(redis_sock, key);

    // Construct command
    *cmd_len = redis_cmd_format_static(cmd, kw, "SSS", key, mem, val);

    CMD_SET_SLOT(slot, key);

	zend_string_release(val);
	zend_string_release(key);

    return SUCCESS;
}

/* HSET */
int redis_hset_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_hset_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, "HSET",
            cmd, cmd_len, slot);
}

/* HSETNX */
int redis_hsetnx_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_hset_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, "HSETNX",
            cmd, cmd_len, slot);
}

/* SRANDMEMBER */
int redis_srandmember_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx,
        short *have_count)
{
	zend_string *key;
    zend_long count;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "S|l", &key, &count) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    // Set our have count flag
    *have_count = ZEND_NUM_ARGS() == 2;

    // Two args means we have the optional COUNT
    if (*have_count) {
        *cmd_len = redis_cmd_format_static(cmd, "SRANDMEMBER", "Sl", key, count);
    } else {
        *cmd_len = redis_cmd_format_static(cmd, "SRANDMEMBER", "S", key);
    }

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    return SUCCESS;
}

/* ZINCRBY */
int redis_zincrby_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
	zend_string *key, *mem;
    double incrby;
    zval *z_val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "Sdz", &key, &incrby, &z_val) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);
    mem = redis_serialize(redis_sock, z_val);

    *cmd_len = redis_cmd_format_static(cmd, "ZINCRBY", "SfS", key, incrby, mem);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);
	zend_string_release(mem);

    return SUCCESS;
}

/* SORT */
int redis_sort_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        int *using_store, char **cmd, int *cmd_len, short *slot,
        void **ctx)
{
    zval *z_opts=NULL, *z_ele, z_argv;
	zend_string *key;
    HashTable *ht_opts;
	long low, high;
	HashTable *ht_argv;
    smart_str cmdstr = {0};

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "S|a", &key, &z_opts) == FAILURE) {
        return FAILURE;
    }

    // Default that we're not using store
    *using_store = 0;

    key = redis_key_prefix(redis_sock, key);

    // If we don't have an options array, the command is quite simple
    if (!z_opts || zend_hash_num_elements(Z_ARRVAL_P(z_opts)) == 0) {
        // Construct command
        *cmd_len = redis_cmd_format_static(cmd, "SORT", "S", key);

        // Push out slot, store flag, and clean up
        *using_store = 0;
        CMD_SET_SLOT(slot, key);

		zend_string_release(key);

        return SUCCESS;
    }

    // Create our hash table to hold our sort arguments
    array_init(&z_argv);

    // SORT <key>
    add_next_index_str(&z_argv, zend_string_copy(key));

    // Set slot
    CMD_SET_SLOT(slot, key);

    // Grab the hash table
    ht_opts = Z_ARRVAL_P(z_opts);

    // Handle BY pattern
    if (((z_ele = zend_hash_str_find(ht_opts, "by", sizeof("by") - 1)) != NULL ||
                (z_ele = zend_hash_str_find(ht_opts, "BY", sizeof("BY") - 1)) != NULL) &&
            Z_TYPE_P(z_ele) == IS_STRING) {
        // "BY" option is disabled in cluster
        if (slot) {
            php_error_docref(NULL, E_WARNING, "SORT BY option is not allowed in Redis Cluster");
			zend_string_release(key);
            zval_dtor(&z_argv);
            return FAILURE;
        }

        // ... BY <pattern>
        add_next_index_stringl(&z_argv, "BY", sizeof("BY")-1);
        add_next_index_str(&z_argv, zend_string_copy(Z_STR_P(z_ele)));
    }

    // Handle ASC/DESC option
    if (((z_ele = zend_hash_str_find(ht_opts, "sort", sizeof("sort") - 1)) != NULL ||
                (z_ele = zend_hash_str_find(ht_opts, "SORT", sizeof("SORT") - 1)) != NULL) &&
            Z_TYPE_P(z_ele) == IS_STRING) {
        // 'asc'|'desc'
        add_next_index_str(&z_argv, zend_string_copy(Z_STR_P(z_ele)));
    }

    // STORE option
    if (((z_ele = zend_hash_str_find(ht_opts, "store", 5)) != NULL ||
                (z_ele = zend_hash_str_find(ht_opts, "STORE", 5)) != NULL) &&
            Z_TYPE_P(z_ele )== IS_STRING) {
        // Slot verification
        int cross_slot = slot && *slot != cluster_hash_key(Z_STRVAL_P(z_ele), Z_STRLEN_P(z_ele));

        if (cross_slot) {
            php_error_docref(0, E_WARNING, "Error, SORT key and STORE key have different slots!");
			zend_string_release(key);
            zval_dtor(&z_argv);
            return FAILURE;
        }

        // STORE <key>
        add_next_index_stringl(&z_argv, "STORE", sizeof("STORE")-1);
        add_next_index_str(&z_argv, zend_string_copy(Z_STR_P(z_ele)));

        // We are using STORE
        *using_store = 1;
    }

    // GET option
    if (((z_ele = zend_hash_str_find(ht_opts, "get", 3)) != NULL ||
                (z_ele = zend_hash_str_find(ht_opts, "GET", 3)) != NULL) &&
            (Z_TYPE_P(z_ele) == IS_STRING || Z_TYPE_P(z_ele) == IS_ARRAY)) {

        // Disabled in cluster
        if (slot) {
            php_error_docref(NULL, E_WARNING, "GET option for SORT disabled in Redis Cluster");
			zend_string_release(key);
            zval_dtor(&z_argv);
            return FAILURE;
        }

        // If it's a string just add it
        if (Z_TYPE_P(z_ele) == IS_STRING) {
            add_next_index_stringl(&z_argv, "GET", sizeof("GET")-1);
            add_next_index_str(&z_argv, zend_string_copy(Z_STR_P(z_ele)));
        } else {
            HashTable *ht_keys = Z_ARRVAL_P(z_ele);
            int added = 0;
            zval *z_key;

            ZEND_HASH_FOREACH_VAL(ht_keys, z_key) {
                if (Z_TYPE_P(z_key) != IS_STRING) {
                    continue;
                }

                /* Add get per thing we're getting */
                add_next_index_stringl(&z_argv,"GET", sizeof("GET")-1);

                // Add this key to our argv array
                add_next_index_str(&z_argv, zend_string_copy(Z_STR_P(z_key)));

                added++;

            } ZEND_HASH_FOREACH_END();

            // Make sure we were able to add at least one
            if (added == 0) {
                php_error_docref(NULL, E_WARNING, "Array of GET values requested, but none are valid");
				zend_string_release(key);
                zval_dtor(&z_argv);
                return FAILURE;
            }
        }
    }

    // ALPHA
    if (((z_ele = zend_hash_str_find(ht_opts, "alpha", 5)) != NULL ||
                (z_ele = zend_hash_str_find(ht_opts, "ALPHA", 5)) != NULL) &&
            Z_TYPE_P(z_ele) == IS_TRUE) {
        add_next_index_stringl(&z_argv, "ALPHA", sizeof("ALPHA")-1);
    }

    // LIMIT <offset> <count>
    if (((z_ele = zend_hash_str_find(ht_opts, "limit", 5)) != NULL ||
                (z_ele = zend_hash_str_find(ht_opts, "LIMIT", 5)) != NULL) &&
            Z_TYPE_P(z_ele) == IS_ARRAY) {
        HashTable *ht_off = Z_ARRVAL_P(z_ele);
        zval *z_off, *z_cnt;

        if ((z_off = zend_hash_index_find(ht_off, 0)) != NULL &&
                (z_cnt = zend_hash_index_find(ht_off, 1)) != NULL) {

            if ((Z_TYPE_P(z_off) != IS_STRING && Z_TYPE_P(z_off) != IS_LONG) ||
                    (Z_TYPE_P(z_cnt) != IS_STRING && Z_TYPE_P(z_cnt) != IS_LONG)) {
                php_error_docref(NULL, E_WARNING, "LIMIT options on SORT command must be longs or strings");
				zend_string_release(key);
                zval_dtor(&z_argv);
                return FAILURE;
            }

            // Add LIMIT argument
            add_next_index_stringl(&z_argv, "LIMIT", sizeof("LIMIT")-1);
            
            if (Z_TYPE_P(z_off) == IS_STRING) {
                low = atol(Z_STRVAL_P(z_off));
            } else {
                low = Z_LVAL_P(z_off);
            }
            if (Z_TYPE_P(z_cnt) == IS_STRING) {
                high = atol(Z_STRVAL_P(z_cnt));
            } else {
                high = Z_LVAL_P(z_cnt);
            }

            // Add our two LIMIT arguments
            add_next_index_long(&z_argv, low);
            add_next_index_long(&z_argv, high);
        }
    }

    // Start constructing our command
    ht_argv = Z_ARRVAL(z_argv);
    redis_cmd_init_sstr(&cmdstr, zend_hash_num_elements(ht_argv), "SORT", sizeof("SORT")-1);

    // Iterate through our arguments
    ZEND_HASH_FOREACH_VAL(ht_argv, z_ele) {
        // Args are strings or longs
        if (Z_TYPE_P(z_ele) == IS_STRING) {
            redis_cmd_append_sstr(&cmdstr, Z_STRVAL_P(z_ele), Z_STRLEN_P(z_ele));
        } else {
            redis_cmd_append_sstr_long(&cmdstr, Z_LVAL_P(z_ele));
        }

    } ZEND_HASH_FOREACH_END();

	zend_string_release(key);
    zval_dtor(&z_argv);

    // Push our length and command
    *cmd_len = ZSTR_LEN(cmdstr.s);
    *cmd     = ZSTR_VAL(cmdstr.s);

    return SUCCESS;
}

/* HDEL */
int redis_hdel_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zval *z_args;
    smart_str cmdstr = {0};
    zend_string *arg, *sarg;
    int i, argc = ZEND_NUM_ARGS();

    // We need at least KEY and one member
    if (argc < 2) {
        return FAILURE;
    }

    // Grab arguments as an array
    z_args = safe_emalloc(sizeof(zval), argc, 0);
    if (zend_get_parameters_array(ht, argc, z_args) == FAILURE) {
        efree(z_args);
        return FAILURE;
    }

    // Get first argument (the key) as a string
	sarg = zval_get_string(&z_args[0]);

    arg = redis_key_prefix(redis_sock, sarg);

    // Start command construction
    redis_cmd_init_sstr(&cmdstr, argc, "HDEL", sizeof("HDEL")-1);
    redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(arg), ZSTR_LEN(arg));

    CMD_SET_SLOT(slot, arg);

	zend_string_release(arg);
	zend_string_release(sarg);

    // Iterate through the members we're removing
    for(i = 1; i < argc; i++) {
		sarg = zval_get_string(&z_args[i]);
        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(sarg), ZSTR_LEN(sarg));
		zend_string_release(sarg);
    }

    // Push out values
    *cmd     = ZSTR_VAL(cmdstr.s);
    *cmd_len = ZSTR_LEN(cmdstr.s);

    efree(z_args);

    return SUCCESS;
}

/* ZADD */
int redis_zadd_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zval *z_args;
	zend_string *zkey, *key, *val;
    int i, argc = ZEND_NUM_ARGS();
    smart_str cmdstr = {0};

    z_args = safe_emalloc(sizeof(zval), argc, 0);
    if (zend_get_parameters_array(ht, argc, z_args) == FAILURE) {
        efree(z_args);
        return FAILURE;
    }

    if (argc < 3 || (argc-1) % 2 != 0) {
        efree(z_args);
        return FAILURE;
    }

    zkey = zval_get_string(&z_args[0]);
    key = redis_key_prefix(redis_sock, zkey);

    // Start command construction
    redis_cmd_init_sstr(&cmdstr, argc, "ZADD", sizeof("ZADD")-1);
    redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(key), ZSTR_LEN(key));

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);
	zend_string_release(zkey);

    // Now the rest of our arguments
    for (i = 1; i < argc; i += 2) {
        val = redis_serialize(redis_sock, &z_args[i+1]);

        // Convert score to a double
        redis_cmd_append_sstr_dbl(&cmdstr, zval_get_double(&z_args[i]));

        redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(val), ZSTR_LEN(val));

        zend_string_release(val);
    }

    // Push output values
    *cmd     = ZSTR_VAL(cmdstr.s);
    *cmd_len = ZSTR_LEN(cmdstr.s);

    efree(z_args);

    return SUCCESS;
}

/* OBJECT */
int redis_object_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        REDIS_REPLY_TYPE *rtype, char **cmd, int *cmd_len,
        short *slot, void **ctx)
{
    zend_string *key, *subcmd;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "SS", &subcmd, &key) == FAILURE) {
        return FAILURE;
    }

    key = redis_key_prefix(redis_sock, key);

    *cmd_len = redis_cmd_format_static(cmd, "OBJECT", "SS", subcmd, key);

    CMD_SET_SLOT(slot, key);

	zend_string_release(key);

    // Push the reply type to our caller
    if (zend_string_equals_literal_ci(subcmd, "refcount") || zend_string_equals_literal_ci(subcmd, "idletime")) {
        *rtype = TYPE_INT;
    } else if (zend_string_equals_literal_ci(subcmd, "encoding")) {
        *rtype = TYPE_BULK;
    } else {
        php_error_docref(NULL, E_WARNING, "Invalid subcommand sent to OBJECT");
        efree(*cmd);
        return FAILURE;
    }

    return SUCCESS;
}

/* DEL */
int redis_del_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_varkey_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "DEL", sizeof("DEL")-1, 1, 0, cmd, cmd_len, slot);
}

/* WATCH */
int redis_watch_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_varkey_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "WATCH", sizeof("WATCH")-1, 1, 0, cmd, cmd_len, slot);
}

/* BLPOP */
int redis_blpop_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_varkey_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "BLPOP", sizeof("BLPOP")-1, 2, 1, cmd, cmd_len, slot);
}

/* BRPOP */
int redis_brpop_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_varkey_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "BRPOP", sizeof("BRPOP")-1, 1, 1, cmd, cmd_len, slot);
}

/* SINTER */
int redis_sinter_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_varkey_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "SINTER", sizeof("SINTER")-1, 1, 0, cmd, cmd_len, slot);
}

/* SINTERSTORE */
int redis_sinterstore_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_varkey_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "SINTERSTORE", sizeof("SINTERSTORE")-1, 2, 0, cmd, cmd_len, slot);
}

/* SUNION */
int redis_sunion_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_varkey_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "SUNION", sizeof("SUNION")-1, 1, 0, cmd, cmd_len, slot);
}

/* SUNIONSTORE */
int redis_sunionstore_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_varkey_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "SUNIONSTORE", sizeof("SUNIONSTORE")-1, 2, 0, cmd, cmd_len, slot);
}

/* SDIFF */
int redis_sdiff_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_varkey_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock, "SDIFF",
            sizeof("SDIFF")-1, 1, 0, cmd, cmd_len, slot);
}

/* SDIFFSTORE */
int redis_sdiffstore_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
        char **cmd, int *cmd_len, short *slot, void **ctx)
{
    return gen_varkey_cmd(INTERNAL_FUNCTION_PARAM_PASSTHRU, redis_sock,
            "SDIFFSTORE", sizeof("SDIFFSTORE")-1, 2, 0, cmd, cmd_len, slot);
}

/* COMMAND */
int redis_rawcommand_cmd(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock,
                         char **cmd, int *cmd_len, short *slot, void **ctx)
{
    zend_string *kw = NULL;
    zval *z_arg;
	zval *z_ele;
	HashTable *ht_arr;
	smart_str cmdstr = {0};

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "|Sz", &kw, &z_arg) == FAILURE) {
        return FAILURE;
    }

    /* Construct our command */
    if (!kw) {
        *cmd_len = redis_cmd_format_static(cmd, "COMMAND", "");
    } else if (kw && !z_arg) {
        /* Sanity check */
        if (!zend_string_equals_literal_ci(kw, "info") || Z_TYPE_P(z_arg) != IS_STRING) {
            return FAILURE;
        }

        /* COMMAND INFO <cmd> */
        *cmd_len = redis_cmd_format_static(cmd, "COMMAND", "sS", "INFO", sizeof("INFO")-1, Z_STR_P(z_arg));
    } else {
        uint32_t arr_len;

        /* Sanity check on args */
        if (!zend_string_equals_literal_ci(kw, "getkeys") ||
				Z_TYPE_P(z_arg) != IS_ARRAY || 
                (arr_len = zend_hash_num_elements(Z_ARRVAL_P(z_arg))) < 1) {
            return FAILURE;
        }

        
        ht_arr = Z_ARRVAL_P(z_arg);

        redis_cmd_init_sstr(&cmdstr, 1 + arr_len, "COMMAND", sizeof("COMMAND")-1);
        redis_cmd_append_sstr(&cmdstr, "GETKEYS", sizeof("GETKEYS")-1);

        ZEND_HASH_FOREACH_VAL(ht_arr, z_ele) {
			zend_string *zkey = zval_get_string(z_ele);
            redis_cmd_append_sstr(&cmdstr, ZSTR_VAL(zkey), ZSTR_LEN(zkey));
			zend_string_release(zkey);
        } ZEND_HASH_FOREACH_END();

        *cmd = ZSTR_VAL(cmdstr.s);
        *cmd_len = ZSTR_LEN(cmdstr.s);
    }

    /* Any slot will do */
    CMD_RAND_SLOT(slot);

    return SUCCESS;
}

/*
 * Redis commands that don't deal with the server at all.  The RedisSock*
 * pointer is the only thing retreived differently, so we just take that
 * in additon to the standard INTERNAL_FUNCTION_PARAMETERS for arg parsing,
 * return value handling, and thread safety. */

void redis_getoption_handler(INTERNAL_FUNCTION_PARAMETERS,
        RedisSock *redis_sock, redisCluster *c)
{
    zend_long option;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &option) == FAILURE) {
        RETURN_FALSE;
    }

    // Return the requested option
    switch(option) {
        case REDIS_OPT_SERIALIZER:
            RETURN_LONG(redis_sock->serializer);
        case REDIS_OPT_PREFIX:
            if (redis_sock->prefix) {
                RETURN_STRINGL(redis_sock->prefix, redis_sock->prefix_len);
            }
            RETURN_NULL();
        case REDIS_OPT_READ_TIMEOUT:
            RETURN_DOUBLE(redis_sock->read_timeout);
        case REDIS_OPT_SCAN:
            RETURN_LONG(redis_sock->scan);
        case REDIS_OPT_FAILOVER:
            RETURN_LONG(c->failover);
        default:
            RETURN_FALSE;
    }
}

void redis_setoption_handler(INTERNAL_FUNCTION_PARAMETERS,
        RedisSock *redis_sock, redisCluster *c)
{
    zend_long option, val_long;
    zend_string *val_str;
    struct timeval read_tv;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "lS", &option, &val_str) == FAILURE) {
        RETURN_FALSE;
    }

    switch (option) {
        case REDIS_OPT_SERIALIZER:
            val_long = atol(ZSTR_VAL(val_str));
            if (val_long == REDIS_SERIALIZER_NONE
#ifdef HAVE_REDIS_IGBINARY
                    || val_long == REDIS_SERIALIZER_IGBINARY
#endif
                    || val_long == REDIS_SERIALIZER_PHP)
            {
                redis_sock->serializer = val_long;
                RETURN_TRUE;
            } else {
                RETURN_FALSE;
            }
            break;
        case REDIS_OPT_PREFIX:
            if (redis_sock->prefix) {
                efree(redis_sock->prefix);
            }
            if (ZSTR_LEN(val_str) == 0) {
                redis_sock->prefix = NULL;
                redis_sock->prefix_len = 0;
            } else {
                redis_sock->prefix_len = ZSTR_LEN(val_str);
                redis_sock->prefix = emalloc(1 + ZSTR_LEN(val_str));
                memcpy(redis_sock->prefix, ZSTR_VAL(val_str), ZSTR_LEN(val_str) + 1);
            }
            RETURN_TRUE;
        case REDIS_OPT_READ_TIMEOUT:
            redis_sock->read_timeout = atof(ZSTR_VAL(val_str));
            if (redis_sock->stream) {
                read_tv.tv_sec  = (time_t)redis_sock->read_timeout;
                read_tv.tv_usec = (int)((redis_sock->read_timeout -
                            read_tv.tv_sec) * 1000000);
                php_stream_set_option(redis_sock->stream,
                        PHP_STREAM_OPTION_READ_TIMEOUT, 0, &read_tv);
            }
            RETURN_TRUE;
        case REDIS_OPT_SCAN:
            val_long = atol(ZSTR_VAL(val_str));
            if (val_long == REDIS_SCAN_NORETRY || val_long == REDIS_SCAN_RETRY) {
                redis_sock->scan = val_long;
                RETURN_TRUE;
            }
            RETURN_FALSE;
            break;
        case REDIS_OPT_FAILOVER:
            val_long = atol(ZSTR_VAL(val_str));
            if (val_long == REDIS_FAILOVER_NONE || 
                val_long == REDIS_FAILOVER_ERROR ||
                val_long == REDIS_FAILOVER_DISTRIBUTE)
            {
                c->failover = val_long;
                RETURN_TRUE;
            } else {
                RETURN_FALSE;
            } 
        default:
            RETURN_FALSE;
    }
}

void redis_prefix_handler(INTERNAL_FUNCTION_PARAMETERS, RedisSock *redis_sock) {
	zend_string *key;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &key) == FAILURE) {
        RETURN_FALSE;
    }

    RETURN_STR(redis_key_prefix(redis_sock, key));
}

void redis_serialize_handler(INTERNAL_FUNCTION_PARAMETERS,
        RedisSock *redis_sock)
{
    zval *z_val;
    zend_string *val;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "z", &z_val)==FAILURE) {
        RETURN_FALSE;
    }

    val = redis_serialize(redis_sock, z_val);

    RETURN_STR(val);
}

void redis_unserialize_handler(INTERNAL_FUNCTION_PARAMETERS,
        RedisSock *redis_sock, zend_class_entry *ex)
{
    zend_string *value;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &value) == FAILURE) {
        RETURN_FALSE;
    }

    // We only need to attempt unserialization if we have a serializer running
    if (redis_sock->serializer != REDIS_SERIALIZER_NONE) {
        if (redis_unserialize(redis_sock, ZSTR_VAL(value), ZSTR_LEN(value), return_value) == 0) {
            // Badly formed input, throw an execption
            zend_throw_exception(ex, "Invalid serialized data, or unserialization error", 0);
            RETURN_FALSE;
        }
    } else {
        RETURN_STR_COPY(value);
    }
}

/* vim: set tabstop=4 softtabstop=4 noexpandtab shiftwidth=4: */
