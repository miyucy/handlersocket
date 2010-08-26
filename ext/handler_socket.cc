#include <ruby.h>
#include "hstcpcli.hpp"

typedef struct {
    dena::hstcpcli_i *ptr;
} HandlerSocket;

void parse_options(dena::config&, dena::socket_args&, VALUE);

VALUE hs_free(HandlerSocket* hs)
{
    fprintf(stderr, "hs=%p\n", hs);
    if (hs) {
        fprintf(stderr, "hs->ptr=%p\n", hs->ptr);
        if (hs->ptr) {
            hs->ptr->close();
            delete hs->ptr;
        }
        xfree(hs);
    }
}

VALUE hs_alloc(VALUE self)
{
    return Data_Wrap_Struct(self, NULL, hs_free, 0);
}

VALUE hs_initialize(VALUE self, VALUE options)
{
    HandlerSocket* hs;

    Data_Get_Struct(self, HandlerSocket, hs);
    fprintf(stderr, "hs=%p\n", hs);
    if (!hs) {
        hs = (HandlerSocket*)xmalloc(sizeof(HandlerSocket));
        MEMZERO(hs, HandlerSocket, 1);
        DATA_PTR(self) = hs;
        fprintf(stderr, "hs=%p\n", hs);
    }

    dena::config      conf;
    dena::socket_args args;
    parse_options(conf, args, options);

    dena::hstcpcli_ptr ptr = dena::hstcpcli_i::create(args);
    fprintf(stderr, "ptr=%p\n", &ptr);
    hs->ptr = ptr.get();
    fprintf(stderr, "hs->ptr=%p\n", hs->ptr);
    fprintf(stderr, "ptr.get()=%p\n", ptr.get());
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

    int retval = hs->ptr->reconnect();
    return INT2FIX(retval);
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

    do {
        size_t pst_id = NUM2INT(id);
        ptr->request_buf_open_index(pst_id, StringValuePtr(db), StringValuePtr(table), StringValuePtr(index), StringValuePtr(fields));
        if (ptr->request_send() != 0) {
            break;
        }

        size_t nflds = 0;
        ptr->response_recv(nflds);
        const int e = ptr->get_error_code();
        fprintf(stderr, "errcode=%d\n", e);
        if (e >= 0) {
            ptr->response_buf_remove();
        }
        fprintf(stderr, "errcode=%d\n", ptr->get_error_code());
    } while(0);

    return INT2FIX(ptr->get_error_code());
}

VALUE hs_execute_single(int argc, VALUE *argv, VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);
    dena::hstcpcli_i *const ptr = hs->ptr;

    VALUE id, op, keys, limit, skip, modop, modvals;
    rb_scan_args(argc, argv, "52", &id, &op, &keys, &limit, &skip, &modop, &modvals);

    StringValue(op);
    Check_Type(keys, T_ARRAY);

    if (!NIL_P(modop)) {
        StringValue(modop);
        Check_Type(modvals, T_ARRAY);
    }

    dena::string_ref op_ref;
    std::vector<dena::string_ref> keyary;

    op_ref = dena::string_ref(RSTRING_PTR(op), RSTRING_LEN(op));

    keyary.reserve(RARRAY_LEN(keys));
    for (size_t i=0; i<RARRAY_LEN(keys); i++) {
        VALUE key = rb_ary_entry(keys, i);
        StringValuePtr(key);
        keyary.push_back(dena::string_ref(RSTRING_PTR(key), RSTRING_LEN(key)));
    }

    dena::string_ref modop_ref;
    std::vector<dena::string_ref> modary;

    if (!NIL_P(modop)) {
        modop_ref = dena::string_ref(RSTRING_PTR(modop), RSTRING_LEN(modop));

        modary.reserve(RARRAY_LEN(modvals));
        for (size_t i=0; i<RARRAY_LEN(modvals); i++) {
            VALUE key = rb_ary_entry(modvals, i);
            // StringValue(key);
            modary.push_back(dena::string_ref(RSTRING_PTR(key), RSTRING_LEN(key)));
        }
    }

    ptr->request_buf_exec_generic(NUM2INT(id),
                                  op_ref,
                                  &keyary[0], keyary.size(),
                                  NUM2INT(limit), NUM2INT(skip),
                                  modop_ref,
                                  &modary[0], modary.size());

    if (ptr->request_send() != 0) {
        return Qnil;
    }

    size_t nflds = 0;
    ptr->response_recv(nflds);
    fprintf(stderr, "nflds=%zu\n", nflds);

    VALUE retval = rb_ary_new();

    const int e = ptr->get_error_code();
    fprintf(stderr, "e=%d nflds=%zu\n", e, nflds);
    rb_ary_push(retval, FIX2INT(e));

    if (e != 0) {
        const std::string s = ptr->get_error();
        rb_ary_push(retval, rb_str_new2(s.c_str()));
    } else {
        VALUE arys, ary, val;

        arys = rb_ary_new();
        const dena::string_ref *row = 0;
        while ((row = ptr->get_next_row()) != 0) {
            fprintf(stderr, "row=%p\n", row);
            ary = rb_ary_new2(nflds);
            for (size_t i = 0; i < nflds; ++i) {
                const dena::string_ref& v = row[i];
                fprintf(stderr, "FLD %zu v=%s vbegin=%p\n", i, std::string(v.begin(), v.size()).c_str(), v.begin());
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

    return retval;
}

VALUE hs_execute_multi(int argc, VALUE *argv, VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);
    dena::hstcpcli_i *const ptr = hs->ptr;

    return Qnil;
}

VALUE hs_execute_find(int argc, VALUE *argv, VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);
    dena::hstcpcli_i *const ptr = hs->ptr;

    return Qnil;
}

VALUE hs_execute_update(int argc, VALUE *argv, VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);
    dena::hstcpcli_i *const ptr = hs->ptr;

    return Qnil;
}

VALUE hs_execute_delete(int argc, VALUE *argv, VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);
    dena::hstcpcli_i *const ptr = hs->ptr;

    return Qnil;
}

VALUE hs_execute_insert(int argc, VALUE *argv, VALUE self)
{
    HandlerSocket* hs;
    Data_Get_Struct(self, HandlerSocket, hs);
    dena::hstcpcli_i *const ptr = hs->ptr;

    return Qnil;
}

void parse_options(dena::config& conf, dena::socket_args& args, VALUE options)
{
    VALUE val;

    Check_Type(options, T_HASH);

    val = rb_hash_aref(options, ID2SYM(rb_intern("host")));
    if(val != Qnil)
    {
        conf["host"] = std::string(StringValuePtr(val));
        fprintf(stderr, "host=%s\n", conf["host"].c_str());
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("port")));
    if(val != Qnil)
    {
        conf["port"] = std::string(StringValuePtr(val));
        fprintf(stderr, "port=%s\n", conf["port"].c_str());
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("timeout")));
    if(val != Qnil)
    {
        conf["timeout"] = std::string(StringValuePtr(val));
        fprintf(stderr, "timeout=%s\n", conf["timeout"].c_str());
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("listen_backlog")));
    if(val != Qnil)
    {
        conf["listen_backlog"] = std::string(StringValuePtr(val));
        fprintf(stderr, "listen_backlog=%s\n", conf["listen_backlog"].c_str());
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("sndbuf")));
    if(val != Qnil)
    {
        conf["sndbuf"] = std::string(StringValuePtr(val));
        fprintf(stderr, "sndbuf=%s\n", conf["sndbuf"].c_str());
    }

    val = rb_hash_aref(options, ID2SYM(rb_intern("rcvbuf")));
    if(val != Qnil)
    {
        conf["rcvbuf"] = std::string(StringValuePtr(val));
        fprintf(stderr, "rcvbuf=%s\n", conf["rcvbuf"].c_str());
    }

    args.set(conf);
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
        rb_define_method(rb_cHandlerSocket, "execute_find", (VALUE(*)(...))hs_execute_find, -1);
        rb_define_method(rb_cHandlerSocket, "execute_update", (VALUE(*)(...))hs_execute_update, -1);
        rb_define_method(rb_cHandlerSocket, "execute_delete", (VALUE(*)(...))hs_execute_delete, -1);
        rb_define_method(rb_cHandlerSocket, "execute_insert", (VALUE(*)(...))hs_execute_insert, -1);
    }
}
