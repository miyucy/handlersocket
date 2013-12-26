#include <ruby.h>
#include "hstcpcli.hpp"

#define GetHandlerSocket(object, handler_socket) {                  \
        Data_Get_Struct((object), HandlerSocket, (handler_socket)); \
}

typedef struct {
    dena::hstcpcli_i* ptr;
    size_t last_id;
} HandlerSocket;

static VALUE rb_cHandlerSocket;
static VALUE rb_eHandlerSocketIOError;
static VALUE rb_eHandlerSocketError;

void
hs_parse_options(dena::socket_args& args, VALUE options)
{
    dena::config conf;
    VALUE val;

    Check_Type(options, T_HASH);

    val = rb_hash_aref(options, ID2SYM(rb_intern("host")));
    if (!NIL_P(val)) {
        conf["host"] = std::string(StringValuePtr(val));
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("port")));
    if (!NIL_P(val)) {
        val = rb_convert_type(val, T_STRING, "String", "to_s");
        conf["port"] = std::string(RSTRING_PTR(val));
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("timeout")));
    if (!NIL_P(val)) {
        conf["timeout"] = std::string(StringValuePtr(val));
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("listen_backlog")));
    if (!NIL_P(val)) {
        conf["listen_backlog"] = std::string(StringValuePtr(val));
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("sndbuf")));
    if (!NIL_P(val)) {
        conf["sndbuf"] = std::string(StringValuePtr(val));
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("rcvbuf")));
    if (!NIL_P(val)) {
        conf["rcvbuf"] = std::string(StringValuePtr(val));
    }

    args.set(conf);

    val = rb_hash_aref(options, ID2SYM(rb_intern("reuseaddr")));
    if (!NIL_P(val)) {
        args.reuseaddr = val == Qtrue;
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("nonblocking")));
    if (!NIL_P(val)) {
        args.nonblocking = val == Qtrue;
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("use_epoll")));
    if (!NIL_P(val)) {
        args.use_epoll = val == Qtrue;
    }
}

void
hs_array_to_vector(VALUE ary, std::vector<dena::string_ref>& vec)
{
    if (NIL_P(ary) || RARRAY_LEN(ary) == 0) {
        return;
    }

    vec.reserve(RARRAY_LEN(ary));

    for (size_t i=0, n=RARRAY_LEN(ary); i<n; i++) {
        VALUE val = rb_ary_entry(ary, i);
        if (FIXNUM_P(val)) {
            val = rb_fix2str(val, 10);
        }
        StringValue(val);
        vec.push_back(dena::string_ref(RSTRING_PTR(val), RSTRING_LEN(val)));
    }
}

VALUE
hs_get_resultset(HandlerSocket* const hs, const size_t nflds)
{
    VALUE arys = rb_ary_new();
    const dena::string_ref *row = 0;

    while ((row = hs->ptr->get_next_row()) != 0) {
        VALUE ary = rb_ary_new2(nflds);
        for (size_t i=0; i<nflds; i++) {
            const dena::string_ref& v = row[i];
            if (v.begin() != 0) {
                rb_ary_push(ary, rb_str_new(v.begin(), v.size()));
            } else {
                rb_ary_push(ary, Qnil);
            }
        }
        rb_ary_push(arys, ary);
    }

    return arys;
}

void
hs_prepare(HandlerSocket* const hs, VALUE id, VALUE op, VALUE keys, VALUE limit, VALUE skip, VALUE modop, VALUE modvals)
{
    StringValue(op);
    Check_Type(keys, T_ARRAY);

    if (!NIL_P(modop)) {
        StringValue(modop);
    }

    dena::string_ref op_ref, modop_ref;
    std::vector<dena::string_ref> keyary, modary;

    op_ref = dena::string_ref(RSTRING_PTR(op), RSTRING_LEN(op));
    hs_array_to_vector(keys, keyary);

    if (!NIL_P(modop)) {
        modop_ref = dena::string_ref(RSTRING_PTR(modop), RSTRING_LEN(modop));
        hs_array_to_vector(modvals, modary);
    }

    if (NIL_P(limit)) {
        limit = INT2FIX(1);
    }

    if (NIL_P(skip)) {
        skip = INT2FIX(0);
    }

    hs->ptr->request_buf_exec_generic(NUM2INT(id),
                                      op_ref,
                                      &keyary[0], keyary.size(),
                                      NUM2INT(limit), NUM2INT(skip),
                                      modop_ref,
                                      &modary[0], modary.size());
}

void
hs_free(HandlerSocket* hs)
{
    if (hs) {
        if (hs->ptr) {
            hs->ptr->close();
            delete hs->ptr;
        }
        xfree(hs);
    }
}

void
hs_mark(HandlerSocket* hs)
{
}

VALUE
hs_alloc(VALUE klass)
{
    HandlerSocket* hs = ALLOC(HandlerSocket);

    return Data_Wrap_Struct(klass, hs_mark, hs_free, hs);
}

VALUE
hs_initialize(VALUE self, VALUE options)
{
    HandlerSocket* hs;

    Data_Get_Struct(self, HandlerSocket, hs);

    dena::socket_args args;
    hs_parse_options(args, options);

    // get struct hstcpcli_i from std::auto_ptr.
    dena::hstcpcli_ptr ptr = dena::hstcpcli_i::create(args);
    hs->ptr = ptr.get();
    ptr.release();

    hs->last_id = 0;

    return self;
}

VALUE
hs_close(VALUE self)
{
    HandlerSocket* hs;

    GetHandlerSocket(self, hs);

    hs->ptr->close();
    hs->last_id = 0;

    return Qnil;
}

VALUE
hs_reconnect(VALUE self)
{
    HandlerSocket* hs;

    GetHandlerSocket(self, hs);

    if (hs->ptr->reconnect() < 0) {
        rb_raise(rb_eHandlerSocketError, "%s", hs->ptr->get_error().c_str());
    }

    return Qnil;
}

/*
  returns true if cli can send a new request
*/
VALUE
hs_stable_point(VALUE self)
{
    HandlerSocket* hs;

    GetHandlerSocket(self, hs);

    return hs->ptr->stable_point() ? Qtrue : Qfalse;
}

VALUE
hs_error(VALUE self)
{
    HandlerSocket* hs;

    GetHandlerSocket(self, hs);

    std::string error = hs->ptr->get_error();

    return error.empty() ? Qnil : rb_str_new2(error.c_str());
}

VALUE
hs_error_code(VALUE self)
{
    HandlerSocket* hs;

    GetHandlerSocket(self, hs);

    return INT2FIX(hs->ptr->get_error_code());
}

VALUE
hs_open_index(VALUE self, VALUE db, VALUE table, VALUE index, VALUE fields)
{
    HandlerSocket* hs;

    GetHandlerSocket(self, hs);

    VALUE  _db     = rb_obj_dup(db);
    VALUE  _table  = rb_obj_dup(table);
    VALUE  _index  = rb_obj_dup(index);
    VALUE  _fields = rb_obj_dup(fields);

    hs->ptr->request_buf_open_index(hs->last_id,
                                    StringValueCStr(_db),
                                    StringValueCStr(_table),
                                    StringValueCStr(_index),
                                    StringValueCStr(_fields));

    if (hs->ptr->get_error_code() < 0) {
        rb_raise(rb_eHandlerSocketIOError, "request_buf_open_index: %s", hs->ptr->get_error().c_str());
    }

    if (hs->ptr->request_send() < 0) {
        rb_raise(rb_eHandlerSocketIOError, "request_send: %s", hs->ptr->get_error().c_str());
    }

    size_t nflds = 0;
    int result = hs->ptr->response_recv(nflds);
    if (result < 0) {
        rb_raise(rb_eHandlerSocketIOError, "response_recv: %s", hs->ptr->get_error().c_str());
    } else if (result != 0) {
        rb_raise(rb_eHandlerSocketError, "response_recv: %s", hs->ptr->get_error().c_str());
    }

    hs->ptr->response_buf_remove();
    if (hs->ptr->get_error_code() < 0) {
        rb_raise(rb_eHandlerSocketError, "response_buf_remove: %s", hs->ptr->get_error().c_str());
    }

    return INT2NUM(hs->last_id++);
}

VALUE
hs_execute_single(int argc, VALUE *argv, VALUE self)
{
    HandlerSocket* hs;

    Data_Get_Struct(self, HandlerSocket, hs);

    VALUE id, op, keys, limit, skip, modop, modvals;
    rb_scan_args(argc, argv, "34", &id, &op, &keys, &limit, &skip, &modop, &modvals);

    hs_prepare(hs, id, op, keys, limit, skip, modop, modvals);
    if (hs->ptr->request_send() < 0) {
        rb_raise(rb_eHandlerSocketIOError, "request_send: %s", hs->ptr->get_error().c_str());
    }

    size_t nflds = 0;
    int   result = hs->ptr->response_recv(nflds);

    if (result < 0) {
        rb_raise(rb_eHandlerSocketIOError, "response_recv: %s", hs->ptr->get_error().c_str());
    } else if (result > 0) {
        hs->ptr->response_buf_remove();
        rb_raise(rb_eHandlerSocketError, "response_recv: %s", hs->ptr->get_error().c_str());
    } else {
        VALUE retval = rb_ary_new2(2);

        rb_ary_push(retval, INT2FIX(result));
        rb_ary_push(retval, hs_get_resultset(hs, nflds));

        hs->ptr->response_buf_remove();
        return retval;
    }

    return Qnil;
}

VALUE
hs_execute_multi(int argc, VALUE *argv, VALUE self)
{
    HandlerSocket* hs;

    Data_Get_Struct(self, HandlerSocket, hs);

    VALUE exec_args;
    VALUE id, op, keys, limit, skip, modop, modvals;
    int arg = rb_scan_args(argc, argv, "16", &id, &op, &keys, &limit, &skip, &modop, &modvals);
    if (arg >= 3) {
        VALUE tmp = rb_ary_new3(7, id, op, keys, limit, skip, modop, modvals);
        exec_args = rb_ary_new3(1, tmp);
    } else if (arg == 1) {
        Check_Type(id, T_ARRAY);
        exec_args = id;
    } else {
        rb_raise(rb_eArgError, "wrong number of arguments");
    }

    for (size_t i=0, n=RARRAY_LEN(exec_args); i<n; i++) {
        VALUE exec_arg = rb_ary_entry(exec_args, i);
        Check_Type(exec_arg, T_ARRAY);

        id      = rb_ary_entry(exec_arg, 0);
        op      = rb_ary_entry(exec_arg, 1);
        keys    = rb_ary_entry(exec_arg, 2);
        limit   = rb_ary_entry(exec_arg, 3);
        skip    = rb_ary_entry(exec_arg, 4);
        modop   = rb_ary_entry(exec_arg, 5);
        modvals = rb_ary_entry(exec_arg, 6);

        hs_prepare(hs, id, op, keys, limit, skip, modop, modvals);
    }
    if (hs->ptr->request_send() < 0) {
        rb_raise(rb_eHandlerSocketIOError, "request_send: %s", hs->ptr->get_error().c_str());
    }

    VALUE retvals = rb_ary_new();

    for (size_t i=0, n=RARRAY_LEN(exec_args); i<n; i++) {
        size_t nflds = 0;
        int   result = hs->ptr->response_recv(nflds);
        VALUE retval = rb_ary_new2(2);

        rb_ary_push(retval, INT2FIX(result));
        if (result != 0) {
            rb_ary_push(retval, rb_str_new2(hs->ptr->get_error().c_str()));
        } else {
            rb_ary_push(retval, hs_get_resultset(hs, nflds));
        }

        if (result >= 0) {
            hs->ptr->response_buf_remove();
        }

        rb_ary_push(retvals, retval);

        if (result < 0) {
            break;
        }
    }

    return retvals;
}

VALUE
hs_execute_update(VALUE self, VALUE id, VALUE op, VALUE keys, VALUE limit, VALUE skip, VALUE modvals)
{
    VALUE argv[7] = {
        id, op, keys, limit, skip, rb_str_new2("U"), modvals,
    };
    return hs_execute_single(7, argv, self);
}

VALUE
hs_execute_delete(VALUE self, VALUE id, VALUE op, VALUE keys, VALUE limit, VALUE skip)
{
    VALUE argv[7] = {
        id, op, keys, limit, skip, rb_str_new2("D"), Qnil,
    };
    return hs_execute_single(7, argv, self);
}

VALUE
hs_execute_insert(VALUE self, VALUE id, VALUE fvals)
{
    VALUE argv[5] = {
        id, rb_str_new2("+"), fvals, INT2FIX(0), INT2FIX(0),
    };
    return hs_execute_single(5, argv, self);
}

VALUE
hs_auth(VALUE self, VALUE secret, VALUE type)
{
    HandlerSocket* hs;

    Data_Get_Struct(self, HandlerSocket, hs);

    hs->ptr->request_buf_auth(StringValuePtr(secret), StringValuePtr(type));
    if (hs->ptr->get_error_code() < 0) {
        rb_raise(rb_eHandlerSocketError, "%s", hs->ptr->get_error().c_str());
    }

    if (hs->ptr->request_send() < 0) {
        rb_raise(rb_eHandlerSocketError, "%s", hs->ptr->get_error().c_str());
    }

    size_t nflds = 0;
    if (hs->ptr->response_recv(nflds) < 0) {
        rb_raise(rb_eHandlerSocketError, "%s", hs->ptr->get_error().c_str());
    }

    hs->ptr->response_buf_remove();
    if (hs->ptr->get_error_code() < 0) {
        rb_raise(rb_eHandlerSocketError, "%s", hs->ptr->get_error().c_str());
    }

    return Qnil;
}

extern "C" {
    void
    Init_handler_socket(void)
    {
        rb_cHandlerSocket = rb_define_class("HandlerSocket", rb_cObject);
        rb_define_alloc_func(rb_cHandlerSocket, hs_alloc);
        rb_define_private_method(rb_cHandlerSocket, "initialize", (VALUE(*)(...))hs_initialize, 1);

        rb_define_method(rb_cHandlerSocket, "close", (VALUE(*)(...))hs_close, 0);
        rb_define_method(rb_cHandlerSocket, "reconnect", (VALUE(*)(...))hs_reconnect, 0);
        rb_define_method(rb_cHandlerSocket, "stable_point", (VALUE(*)(...))hs_stable_point, 0);
        rb_define_method(rb_cHandlerSocket, "auth", (VALUE(*)(...))hs_auth, 2);

        rb_define_method(rb_cHandlerSocket, "error", (VALUE(*)(...))hs_error, 0);
        rb_define_method(rb_cHandlerSocket, "error_code", (VALUE(*)(...))hs_error_code, 0);

        rb_define_method(rb_cHandlerSocket, "open_index", (VALUE(*)(...))hs_open_index, 4);
        rb_define_method(rb_cHandlerSocket, "execute_single", (VALUE(*)(...))hs_execute_single, -1);
        rb_define_alias(rb_cHandlerSocket, "execute_find", "execute_single");
        rb_define_method(rb_cHandlerSocket, "execute_multi", (VALUE(*)(...))hs_execute_multi, -1);
        rb_define_method(rb_cHandlerSocket, "execute_update", (VALUE(*)(...))hs_execute_update, 6);
        rb_define_method(rb_cHandlerSocket, "execute_delete", (VALUE(*)(...))hs_execute_delete, 5);
        rb_define_method(rb_cHandlerSocket, "execute_insert", (VALUE(*)(...))hs_execute_insert, 2);

        rb_eHandlerSocketIOError = rb_define_class_under(rb_cHandlerSocket, "IOError", rb_eStandardError);
        rb_eHandlerSocketError = rb_define_class_under(rb_cHandlerSocket, "Error", rb_eStandardError);
    }
}
