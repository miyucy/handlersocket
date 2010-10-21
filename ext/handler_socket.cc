#include <ruby.h>
#include "hstcpcli.hpp"

typedef struct {
    dena::hstcpcli_i *ptr;
} HandlerSocket;

void hs_free(HandlerSocket* hs)
{
    if (hs) {
        if (hs->ptr) {
            hs->ptr->close();
            delete hs->ptr;
        }
        xfree(hs);
    }
}

void hs_mark(HandlerSocket* hs)
{
}

VALUE hs_alloc(VALUE self)
{
    return Data_Wrap_Struct(self, hs_mark, hs_free, 0);
}

void parse_options(dena::config& conf, dena::socket_args& args, VALUE options)
{
    VALUE val;

    Check_Type(options, T_HASH);

    val = rb_hash_aref(options, ID2SYM(rb_intern("host")));
    if(val != Qnil)
    {
        conf["host"] = std::string(StringValuePtr(val));
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("port")));
    if(val != Qnil)
    {
        conf["port"] = std::string(StringValuePtr(val));
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("timeout")));
    if(val != Qnil)
    {
        conf["timeout"] = std::string(StringValuePtr(val));
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("listen_backlog")));
    if(val != Qnil)
    {
        conf["listen_backlog"] = std::string(StringValuePtr(val));
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("sndbuf")));
    if(val != Qnil)
    {
        conf["sndbuf"] = std::string(StringValuePtr(val));
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("rcvbuf")));
    if(val != Qnil)
    {
        conf["rcvbuf"] = std::string(StringValuePtr(val));
    }

    args.set(conf);
}

VALUE hs_initialize(VALUE self, VALUE options)
{
    HandlerSocket* hs;

    Data_Get_Struct(self, HandlerSocket, hs);
    if (!hs) {
        hs = (HandlerSocket*)xmalloc(sizeof(HandlerSocket));
        MEMZERO(hs, HandlerSocket, 1);
        DATA_PTR(self) = hs;
    }

    dena::config      conf;
    dena::socket_args args;
    parse_options(conf, args, options);

    dena::hstcpcli_ptr ptr = dena::hstcpcli_i::create(args);
    hs->ptr = ptr.get();
    ptr.release();

    return self;
}

VALUE hs_close(VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);

    hs->ptr->close();
    return Qnil;
}

VALUE hs_reconnect(VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);

    return INT2FIX(hs->ptr->reconnect());
}

VALUE hs_stable_point(VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);

    return hs->ptr->stable_point() ? Qtrue : Qfalse;
}

VALUE hs_error(VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);

    VALUE val = Qnil;
    std::string error = hs->ptr->get_error();

    if (!error.empty()) {
        val = rb_str_new2(error.c_str());
    }
    return val;
}

VALUE hs_error_code(VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);

    return INT2FIX(hs->ptr->get_error_code());
}

VALUE hs_open_index(VALUE self, VALUE id, VALUE db, VALUE table, VALUE index, VALUE fields)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);
    dena::hstcpcli_i *const ptr = hs->ptr;
    size_t  _id = NUM2INT(id);
    VALUE   _db = rb_obj_dup(db),
        _table  = rb_obj_dup(table),
        _index  = rb_obj_dup(index),
        _fields = rb_obj_dup(fields);
    size_t nflds;

    ptr->request_buf_open_index(_id,
                                StringValueCStr(_db),
                                StringValueCStr(_table),
                                StringValueCStr(_index),
                                StringValueCStr(_fields));
    if(ptr->get_error_code() < 0)
    {
        return hs_error_code(self);
    }

    if (ptr->request_send() != 0) {
        return hs_error_code(self);
    }

    if (ptr->response_recv(nflds) != 0) {
        return hs_error_code(self);
    }

    ptr->response_buf_remove();
    if(ptr->get_error_code() < 0)
    {
        return hs_error_code(self);
    }

    return hs_error_code(self);
}

VALUE ary_to_vector(VALUE ary, std::vector<dena::string_ref>& vec)
{
    VALUE ret, val;

    if (NIL_P(ary) || RARRAY_LEN(ary) == 0) {
        return ary;
    }

    ret = rb_ary_new2(RARRAY_LEN(ary));
    vec.reserve(RARRAY_LEN(ary));

    for (size_t i=0; i<RARRAY_LEN(ary); i++) {
        val = rb_ary_entry(ary, i);
        if (FIXNUM_P(val)) {
            val = rb_fix2str(val, 10);
        }
        StringValue(val);
        vec.push_back(dena::string_ref(RSTRING_PTR(val), RSTRING_LEN(val)));
        rb_ary_push(ret, val);
    }

    return ret;
}

VALUE hs_get_resultset(dena::hstcpcli_i *const ptr, size_t nflds)
{
    VALUE arys, ary, val;

    arys = rb_ary_new();
    const dena::string_ref *row = 0;
    while ((row = ptr->get_next_row()) != 0) {
        ary = rb_ary_new2(nflds);
        for (size_t i = 0; i < nflds; ++i) {
            const dena::string_ref& v = row[i];
            if (v.begin() != 0) {
                val = rb_str_new(v.begin(), v.size());
                rb_ary_push(ary, val);
            } else {
                rb_ary_push(ary, Qnil);
            }
        }
        rb_ary_push(arys, ary);
    }
    return arys;
}

void hs_prepare(dena::hstcpcli_i *const ptr, VALUE id, VALUE op, VALUE keys, VALUE limit, VALUE skip, VALUE modop, VALUE modvals)
{
    StringValue(op);
    Check_Type(keys, T_ARRAY);

    if (!NIL_P(modop)) {
        StringValue(modop);
    }

    dena::string_ref op_ref, modop_ref;
    std::vector<dena::string_ref> keyary, modary;

    op_ref = dena::string_ref(RSTRING_PTR(op), RSTRING_LEN(op));
    keys = ary_to_vector(keys, keyary);

    if (!NIL_P(modop)) {
        modop_ref = dena::string_ref(RSTRING_PTR(modop), RSTRING_LEN(modop));
        modvals = ary_to_vector(modvals, modary);
    }

    ptr->request_buf_exec_generic(NUM2INT(id),
                                  op_ref,
                                  &keyary[0], keyary.size(),
                                  NUM2INT(limit), NUM2INT(skip),
                                  modop_ref,
                                  &modary[0], modary.size());
}

VALUE hs_execute_single(int argc, VALUE *argv, VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);
    dena::hstcpcli_i *const ptr = hs->ptr;

    VALUE id, op, keys, limit, skip, modop, modvals;
    rb_scan_args(argc, argv, "52", &id, &op, &keys, &limit, &skip, &modop, &modvals);

    hs_prepare(ptr, id, op, keys, limit, skip, modop, modvals);
    if (ptr->request_send() != 0) {
        return Qnil;
    }

    VALUE retval = rb_ary_new2(2);
    size_t nflds = 0;

    if (ptr->response_recv(nflds) != 0) {
        rb_ary_push(retval, FIX2INT(ptr->get_error_code()));
        rb_ary_push(retval, rb_str_new2(ptr->get_error().c_str()));
        return retval;
    }

    rb_ary_push(retval, FIX2INT(ptr->get_error_code()));
    rb_ary_push(retval, hs_get_resultset(ptr, nflds));

    ptr->response_buf_remove();
    if(ptr->get_error_code() < 0)
    {
    }

    return retval;
}

VALUE hs_execute_multi(int argc, VALUE *argv, VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);
    dena::hstcpcli_i *const ptr = hs->ptr;

    VALUE exec_args, exec_arg;

    VALUE id, op, keys, limit, skip, modop, modvals;
    int arg = rb_scan_args(argc, argv, "16", &id, &op, &keys, &limit, &skip, &modop, &modvals);
    if (arg >= 5) {
        VALUE tmp = rb_ary_new3(id, op, keys, limit, skip, modop, modvals);
        exec_args = rb_ary_new3(tmp);
    } else if (arg == 1) {
        Check_Type(id, T_ARRAY);
        exec_args = id;
    } else {
        rb_raise(rb_eArgError, "wrong number of arguments");
    }

    for (size_t i=0; i<RARRAY_LEN(exec_args); i++) {
        exec_arg = rb_ary_entry(exec_args, i);
        Check_Type(exec_arg, T_ARRAY);

        id      = rb_ary_entry(exec_arg, 0);
        op      = rb_ary_entry(exec_arg, 1);
        keys    = rb_ary_entry(exec_arg, 2);
        limit   = rb_ary_entry(exec_arg, 3);
        skip    = rb_ary_entry(exec_arg, 4);
        modop   = rb_ary_entry(exec_arg, 5);
        modvals = rb_ary_entry(exec_arg, 6);

        StringValue(op);
        Check_Type(keys, T_ARRAY);

        if (!NIL_P(modop)) {
            StringValue(modop);
        }

        dena::string_ref op_ref, modop_ref;
        std::vector<dena::string_ref> keyary, modary;

        op_ref = dena::string_ref(RSTRING_PTR(op), RSTRING_LEN(op));
        keys = ary_to_vector(keys, keyary);
        rb_gc_register_address(&keys);

        if (!NIL_P(modop)) {
            modop_ref = dena::string_ref(RSTRING_PTR(modop), RSTRING_LEN(modop));
            modvals = ary_to_vector(modvals, modary);
            rb_gc_register_address(&modvals);
        }

        ptr->request_buf_exec_generic(NUM2INT(id),
                                      op_ref,
                                      &keyary[0], keyary.size(),
                                      NUM2INT(limit), NUM2INT(skip),
                                      modop_ref,
                                      &modary[0], modary.size());
    }

    VALUE retvals, retval;
    retvals = rb_ary_new();

    if (ptr->request_send() < 0) {
        retval = rb_ary_new();

        const int e = ptr->get_error_code();
        const std::string s = ptr->get_error();
        rb_ary_push(retval, FIX2INT(e));
        rb_ary_push(retval, rb_str_new2(s.c_str()));

        rb_ary_push(retvals, retval);
        return retvals;
    }

    for (size_t i=0; i<RARRAY_LEN(exec_args); i++) {
        retval = rb_ary_new();

        size_t nflds = 0;
        const int e = ptr->response_recv(nflds);
        rb_ary_push(retval, FIX2INT(e));
        if (e != 0) {
            const std::string s = ptr->get_error();
            rb_ary_push(retval, rb_str_new2(s.c_str()));
        } else {
            VALUE arys, ary, val;

            arys = rb_ary_new();
            const dena::string_ref *row = 0;
            while ((row = ptr->get_next_row()) != 0) {
                ary = rb_ary_new2(nflds);
                for (size_t i = 0; i < nflds; ++i) {
                    const dena::string_ref& v = row[i];
                    if (v.begin() != 0) {
                        val = rb_str_new(v.begin(), v.size());
                        rb_ary_push(ary, val);
                    } else {
                        rb_ary_push(ary, Qnil);
                    }
                }
                rb_ary_push(arys, ary);
            }
            rb_ary_push(retval, arys);
        }

        if (e >= 0) {
            ptr->response_buf_remove();
        }

        rb_ary_push(retvals, retval);

        if (e < 0) {
            return retvals;
        }
    }

    return retvals;
}


VALUE hs_execute_update(VALUE self, VALUE id, VALUE op, VALUE keys, VALUE limit, VALUE skip, VALUE modvals)
{
    VALUE argv[7] = {
        id, op, keys, limit, skip, rb_str_new2("U"), modvals,
    };
    return hs_execute_single(7, argv, self);
}

VALUE hs_execute_delete(VALUE self, VALUE id, VALUE op, VALUE keys, VALUE limit, VALUE skip)
{
    VALUE argv[7] = {
        id, op, keys, limit, skip, rb_str_new2("D"), Qnil,
    };
    return hs_execute_single(7, argv, self);
}

VALUE hs_execute_insert(VALUE self, VALUE id, VALUE fvals)
{
    VALUE argv[5] = {
        id, rb_str_new2("+"), fvals, 0, 0,
    };
    return hs_execute_single(5, argv, self);
}

extern "C" {
    void Init_handler_socket(void)
    {
        VALUE rb_cHandlerSocket = rb_define_class("HandlerSocket", rb_cObject);

        rb_define_alloc_func(rb_cHandlerSocket, hs_alloc);
        rb_define_private_method(rb_cHandlerSocket, "initialize", (VALUE(*)(...))hs_initialize, 1);

        rb_define_method(rb_cHandlerSocket, "close", (VALUE(*)(...))hs_close, 0);
        rb_define_method(rb_cHandlerSocket, "reconnect", (VALUE(*)(...))hs_reconnect, 0);
        rb_define_method(rb_cHandlerSocket, "stable_point", (VALUE(*)(...))hs_stable_point, 0);

        rb_define_method(rb_cHandlerSocket, "error", (VALUE(*)(...))hs_error, 0);
        rb_define_method(rb_cHandlerSocket, "error_code", (VALUE(*)(...))hs_error_code, 0);

        rb_define_method(rb_cHandlerSocket, "open_index", (VALUE(*)(...))hs_open_index, 5);
        rb_define_method(rb_cHandlerSocket, "execute_single", (VALUE(*)(...))hs_execute_single, -1);
        rb_define_method(rb_cHandlerSocket, "execute_multi", (VALUE(*)(...))hs_execute_multi, -1);
        rb_define_alias(rb_cHandlerSocket, "execute_find", "execute_single");
        rb_define_method(rb_cHandlerSocket, "execute_update", (VALUE(*)(...))hs_execute_update, 6);
        rb_define_method(rb_cHandlerSocket, "execute_delete", (VALUE(*)(...))hs_execute_delete, 5);
        rb_define_method(rb_cHandlerSocket, "execute_insert", (VALUE(*)(...))hs_execute_insert, 2);
    }
}
