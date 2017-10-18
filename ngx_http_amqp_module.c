#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_md5.h>
#include <amqp_tcp_socket.h>
#include <amqp_ssl_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <syslog.h>


typedef struct{
    ngx_str_t amqp_ip;
    ngx_uint_t amqp_port;
    ngx_str_t amqp_exchange;
    ngx_str_t amqp_routing_key;
    ngx_str_t amqp_user;
    ngx_str_t amqp_password;
    amqp_socket_t* socket;
    amqp_connection_state_t conn;
    ngx_uint_t init;
    ngx_uint_t amqp_debug;
    ngx_uint_t amqp_ssl;
    ngx_uint_t amqp_ssl_verify_peer;
    ngx_uint_t amqp_ssl_verify_hostname;
    ngx_str_t amqp_ssl_ca_certificate;
    ngx_str_t amqp_ssl_client_certificate;
    ngx_str_t amqp_ssl_client_certificate_key;
    ngx_uint_t amqp_ssl_min_version;
    ngx_uint_t amqp_ssl_max_version;
    ngx_str_t script_source;
    ngx_array_t* lengths;
    ngx_array_t* values;
}ngx_http_amqp_conf_t;

static ngx_conf_enum_t  amqp_ssl_version[] = {
    { ngx_string("tlsv1"),   AMQP_TLSv1 },
    { ngx_string("tlsv1.1"), AMQP_TLSv1_1 },
    { ngx_string("tlsv1.2"), AMQP_TLSv1_2 },
    { ngx_string("latest"),  AMQP_TLSvLATEST },
    { ngx_null_string, 0 }
};

static void ngx_http_amqp_exit(ngx_cycle_t* cycle);
static char * ngx_http_amqp(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void* ngx_http_amqp_create_conf(ngx_conf_t *cf);
static char* ngx_http_amqp_merge_conf(ngx_conf_t *cf, void* parent, void* child);
ngx_int_t ngx_http_amqp_handler(ngx_http_request_t* r);


static ngx_command_t ngx_http_amqp_commands[] = {
    {
        ngx_string("amqp_publish"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_amqp,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },
    {
        ngx_string("amqp_ip"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_ip),
        NULL
    },
    {
        ngx_string("amqp_port"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_port),
        NULL
    },
    {
        ngx_string("amqp_exchange"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_exchange),
        NULL
    },
    {
        ngx_string("amqp_routing_key"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_routing_key),
        NULL
    },
    {
        ngx_string("amqp_user"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_user),
        NULL
    },
    {
        ngx_string("amqp_password"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_password),
        NULL
    },
    {
        ngx_string("amqp_debug"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_debug),
        NULL
    },
    {
        ngx_string("amqp_ssl"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_ssl),
        NULL
    },
    {
        ngx_string("amqp_ssl_verify_peer"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_ssl_verify_peer),
        NULL
    },
    {
        ngx_string("amqp_ssl_verify_hostname"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_ssl_verify_hostname),
        NULL
    },
    {
        ngx_string("amqp_ssl_ca_certificate"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_ssl_ca_certificate),
        NULL
    },
    {
        ngx_string("amqp_ssl_client_certificate"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_ssl_client_certificate),
        NULL
    },
    {
        ngx_string("amqp_ssl_client_certificate_key"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_ssl_client_certificate_key),
        NULL
    },
    { ngx_string("amqp_ssl_min_version"),
      NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_enum_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_amqp_conf_t, amqp_ssl_min_version),
      &amqp_ssl_version 
    },
    { ngx_string("amqp_ssl_max_version"),
      NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_enum_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_amqp_conf_t, amqp_ssl_max_version),
      &amqp_ssl_version
     },
    ngx_null_command
};


static ngx_http_module_t ngx_http_amqp_module_ctx = {
    NULL,                          /* preconfiguration */
    NULL,                         /* postconfiguration */

    NULL,                          /* create main configuration */
    NULL,                          /* init main configuration */

    NULL,                          /* create server configuration */
    NULL,                          /* merge server configuration */

    ngx_http_amqp_create_conf,   /* create location configuration */
    ngx_http_amqp_merge_conf     /* merge location configuration */
};

ngx_module_t ngx_http_amqp_module = {
    NGX_MODULE_V1,
    &ngx_http_amqp_module_ctx,    /* module context */
    ngx_http_amqp_commands,       /* module directives */
    NGX_HTTP_MODULE,               /* module type */
    NULL,                          /* init master */
    NULL,                          /* init module */
    NULL,                          /* init process */
    NULL,                          /* init thread */
    NULL,                          /* exit thread */
    ngx_http_amqp_exit,            /* exit process */
    NULL,                          /* exit master */
    NGX_MODULE_V1_PADDING
};


int get_error(int x, u_char const *context, u_char* error)
{
    if (x < 0) {
        syslog(LOG_ERR, "%s: %s\n", context, amqp_error_string2(x));
        ngx_sprintf(error, "%s: %s\n", context, amqp_error_string2(x));
        return 1;
    }
    return 0;
}


int get_amqp_error(amqp_rpc_reply_t x, u_char const *context, u_char** error)
{
    switch (x.reply_type) {
        case AMQP_RESPONSE_NORMAL:
        return 0;

        case AMQP_RESPONSE_NONE:
        syslog(LOG_ERR, "%s: missing RPC reply type!", context);
        ngx_sprintf(*error, "%s: missing RPC reply type!\n", context);
        break;

        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        syslog(LOG_ERR, "%s: %s", context, amqp_error_string2(x.library_error));
        ngx_sprintf(*error, "%s: %s\n", context, amqp_error_string2(x.library_error));
        break;

        case AMQP_RESPONSE_SERVER_EXCEPTION:
        switch (x.reply.id) {
            case AMQP_CONNECTION_CLOSE_METHOD: {
                amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
                syslog(LOG_ERR, "%s: server connection error %d, message: %.*s",
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
                ngx_sprintf(*error, "%s: server connection error %d, message: %.*s\n",
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
                break;
            }
            case AMQP_CHANNEL_CLOSE_METHOD: {
                amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
                syslog(LOG_ERR,  "%s: server channel error %d, message: %.*s",
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
                ngx_sprintf(*error, "%s: server channel error %d, message: %.*s\n",
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
                break;
            }
            default:
            syslog(LOG_ERR, "%s: unknown server error, method id 0x%08X", context, x.reply.id);
            ngx_sprintf(*error, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
            break;
        }
        break;
    }

    return 1;
}

int connect_amqp(ngx_http_amqp_conf_t* amcf, u_char** error){
    int status;
    amqp_rpc_reply_t reply;
        //initialize connection and channel
    amcf->conn=amqp_new_connection();
    
    if (amcf->amqp_ssl) {
      amcf->socket = amqp_ssl_socket_new(amcf->conn);
      if (!amcf->socket) {
        *error=(u_char*)"creating SSL/TLS socket";
        return -1;
      }
      amqp_ssl_socket_set_verify_peer(amcf->socket, amcf->amqp_ssl_verify_peer);
      amqp_ssl_socket_set_verify_hostname(amcf->socket, amcf->amqp_ssl_verify_hostname);
      amqp_status_enum ssl_status = amqp_ssl_socket_set_ssl_versions(amcf->socket, amcf->amqp_ssl_min_version, amcf->amqp_ssl_max_version);
      
      switch(ssl_status) {
        case AMQP_STATUS_INVALID_PARAMETER : 
            *error=(u_char*)"setting SSL protocol, min is higher than max";    
            return -1;
        case AMQP_STATUS_UNSUPPORTED :
            *error=(u_char*)"setting SSL protocol, not supported";    
            return -1;
        default :
            break;
      }
      
      if(amcf->amqp_ssl_ca_certificate.len > 0) {
        status = amqp_ssl_socket_set_cacert(amcf->socket, (char*)amcf->amqp_ssl_ca_certificate.data);
        if (status) {
          *error=(u_char*)"setting CA certificate";
          return -1;
        }
      }
      if(amcf->amqp_ssl_client_certificate.len > 0 && amcf->amqp_ssl_client_certificate_key.len > 0 ) {
        status = amqp_ssl_socket_set_key(amcf->socket, (char*)amcf->amqp_ssl_client_certificate.data, (char*)amcf->amqp_ssl_client_certificate_key.data);
        if (status) {
          *error=(u_char*)"setting client certificate";
          return -1;
        }
      }
    } else {
      amcf->socket=amqp_tcp_socket_new(amcf->conn);
      if(!amcf->socket){
          *error=(u_char*)"Creating TCP socket";
          return -1;
      }
    }
    
    status=amqp_socket_open(amcf->socket, (char*)amcf->amqp_ip.data, (int)amcf->amqp_port);
    if(status){
        *error=(u_char*)"Opening TCP socket";
        return -1;
    }
    reply=amqp_login(amcf->conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,(char*)amcf->amqp_user.data, (char*)amcf->amqp_password.data);
    if(get_amqp_error(reply, (u_char*)"Logging in", error)){
        return -1;
    }
    amqp_channel_open(amcf->conn, 1);
    if(get_amqp_error(amqp_get_rpc_reply(amcf->conn), (u_char*)"Opening channel", error)){
        return -1;
    }
    return 0;
}





ngx_int_t ngx_http_amqp_handler(ngx_http_request_t* r){

    ngx_http_amqp_conf_t* amcf=ngx_http_get_module_loc_conf(r, ngx_http_amqp_module);
    ngx_chain_t out;
    ngx_buf_t* b;
    ngx_str_t messagebody;
    ngx_int_t rc;

    u_char* msg;    

    if(amcf->lengths==NULL){
        messagebody.data=ngx_palloc(r->pool, amcf->script_source.len);
        ngx_memcpy(messagebody.data, amcf->script_source.data, amcf->script_source.len);
        messagebody.len=amcf->script_source.len;
    }
    else{                                  
        if(ngx_http_script_run(r, &messagebody, amcf->lengths->elts, 0, amcf->values->elts)==NULL){
            return NGX_ERROR;
        }  
    }
  
    msg=ngx_pcalloc(r->pool, 4096);
    if(!amcf->init){
        amcf->init=1;
        if(connect_amqp(amcf, &msg)<0) goto error;
    }
    
    amqp_basic_properties_t props;

    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2;
    if(get_error(amqp_basic_publish(amcf->conn, 1, amqp_cstring_bytes((char*)amcf->amqp_exchange.data), amqp_cstring_bytes((char*)amcf->amqp_routing_key.data), 0, 0, &props, amqp_cstring_bytes((char*)messagebody.data)), (u_char*)"Publishing", &msg)){
        syslog(LOG_WARNING, "Cannot publish. Try to republish.");
        memset(msg, 0, sizeof(msg)+1);
        if(connect_amqp(amcf, &msg)<0) goto error;
        if(get_error(amqp_basic_publish(amcf->conn, 1, amqp_cstring_bytes((char*)amcf->amqp_exchange.data), amqp_cstring_bytes((char*)amcf->amqp_routing_key.data), 0, 0, &props, amqp_cstring_bytes((char*)messagebody.data)), (u_char*)"Publishing", &msg)){
            goto error;
        }

    }
    ngx_sprintf(msg, "NO ERROR init=%d", amcf->init); 

    r->headers_out.content_type_len = sizeof("text/html") - 1;
    r->headers_out.content_type.data = (u_char *) "text/html";

    b=ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
    if(b==NULL) return NGX_HTTP_INTERNAL_SERVER_ERROR;
    out.buf=b;
    out.next=NULL;

    if(amcf->amqp_debug){
        b->pos=msg;
        b->last=msg + ngx_strlen(msg);
        r->headers_out.content_length_n=b->last - b->pos;
    }
    else{
        msg=(u_char*)"\n";
        b->pos=msg;
        b->last=msg+ngx_strlen(msg);
        r->headers_out.content_length_n=sizeof(msg);
    }
    b->memory=1;
    b->last_buf=1;

    r->headers_out.status=NGX_HTTP_OK;


    rc=ngx_http_send_header(r);
    if(rc==NGX_ERROR||rc>NGX_OK||r->header_only){
        return rc;
    }


    return ngx_http_output_filter(r, &out);
////////////////////////////////
    error:
    amcf->init=0;
    b=ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
    if(b==NULL) return NGX_HTTP_INTERNAL_SERVER_ERROR;
    out.buf=b;
    out.next=NULL;

    if(amcf->amqp_debug){
      b->pos=msg;
      b->last=msg + ngx_strlen(msg);
      r->headers_out.content_length_n=b->last - b->pos;
    }
    else{
        msg=(u_char*)"Error!";
        b->pos=msg;
        b->last=msg + sizeof(msg);
        r->headers_out.content_length_n=b->last - b->pos;
    }
    b->memory=1;
    b->last_buf=1;

    r->headers_out.status=NGX_HTTP_OK;

    rc=ngx_http_send_header(r);
    if(rc==NGX_ERROR||rc>NGX_OK||r->header_only){
        return rc;
    }
    return ngx_http_output_filter(r, &out);
}

static char * ngx_http_amqp(ngx_conf_t *cf, ngx_command_t *cmd, void *conf){
    if(cf->args->nelts!=2) return NGX_CONF_ERROR;
    ngx_http_core_loc_conf_t *clcf;
    ngx_http_amqp_conf_t* amcf=conf;
    ngx_str_t* val=cf->args->elts;
    ngx_http_script_compile_t sc;
    ngx_uint_t n;


    amcf->init=0;
    openlog("amqp-publish", LOG_CONS|LOG_PID, LOG_LOCAL0);



    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_amqp_handler;




    n=ngx_http_script_variables_count(&val[1]);
    amcf->script_source.data=val[1].data;
    amcf->script_source.len=val[1].len;
    if(n>0){
        ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));

        sc.cf=cf;
        sc.source=&val[1];
        sc.lengths=&amcf->lengths;
        sc.values=&amcf->values;
        sc.variables=n;
        sc.complete_lengths=1;
        sc.complete_values=1;
        if(ngx_http_script_compile(&sc)!=NGX_OK) return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static void* ngx_http_amqp_create_conf(ngx_conf_t *cf){
    ngx_http_amqp_conf_t* conf;

    conf=(ngx_http_amqp_conf_t*)ngx_pcalloc(cf->pool,
        sizeof(ngx_http_amqp_conf_t));
    if(conf==NULL){
        return NULL;
    }

    conf->amqp_debug=NGX_CONF_UNSET_UINT;
    conf->amqp_ssl=NGX_CONF_UNSET_UINT;
    conf->amqp_ssl_verify_peer=NGX_CONF_UNSET_UINT;
    conf->amqp_ssl_verify_hostname=NGX_CONF_UNSET_UINT;
    conf->amqp_ssl_min_version=NGX_CONF_UNSET_UINT;
    conf->amqp_ssl_max_version=NGX_CONF_UNSET_UINT;
    conf->init=NGX_CONF_UNSET_UINT;
    conf->socket=NULL;
    conf->amqp_port=NGX_CONF_UNSET_UINT;
    conf->lengths=NULL;
    conf->values=NULL;

    return conf;
}

static char* ngx_http_amqp_merge_conf(ngx_conf_t *cf, void* parent, void* child){

    ngx_http_amqp_conf_t* prev=parent;
    ngx_http_amqp_conf_t* conf=child;

    ngx_conf_merge_str_value(conf->amqp_ip, prev->amqp_ip, "127.0.0.1");
    ngx_conf_merge_uint_value(conf->amqp_port, prev->amqp_port, 5672);
    ngx_conf_merge_str_value(conf->amqp_exchange, prev->amqp_exchange, "defaultExchange");
    ngx_conf_merge_str_value(conf->amqp_routing_key, prev->amqp_routing_key, "defaultRoutingKey");
    ngx_conf_merge_str_value(conf->amqp_user, prev->amqp_user, "guest");
    ngx_conf_merge_str_value(conf->amqp_password, prev->amqp_password, "guest");
    ngx_conf_merge_uint_value(conf->init, prev->init, 0);
    ngx_conf_merge_uint_value(conf->amqp_debug, prev->amqp_debug, 0);
    ngx_conf_merge_uint_value(conf->amqp_ssl, prev->amqp_ssl, 0);
    ngx_conf_merge_uint_value(conf->amqp_ssl_verify_peer, prev->amqp_ssl_verify_peer, 0);
    ngx_conf_merge_uint_value(conf->amqp_ssl_verify_hostname, prev->amqp_ssl_verify_hostname, 0);
    ngx_conf_merge_str_value(conf->amqp_ssl_ca_certificate, prev->amqp_ssl_ca_certificate, "");
    ngx_conf_merge_str_value(conf->amqp_ssl_client_certificate, prev->amqp_ssl_client_certificate, "");
    ngx_conf_merge_str_value(conf->amqp_ssl_client_certificate_key, prev->amqp_ssl_client_certificate_key, "");
    ngx_conf_merge_uint_value(conf->amqp_ssl_min_version, prev->amqp_ssl_min_version, AMQP_TLSv1_1);
    ngx_conf_merge_uint_value(conf->amqp_ssl_max_version, prev->amqp_ssl_max_version, AMQP_TLSvLATEST);

    if(conf->amqp_ssl)  {
      if (conf->amqp_ssl_client_certificate.len == 0 && conf->amqp_ssl_client_certificate_key.len > 0 ) {
          ngx_log_error(NGX_LOG_EMERG, cf->log, 0,
                        "no amqp_ssl_client_certificate provided");
          return NGX_CONF_ERROR;
      }
      
      if (conf->amqp_ssl_client_certificate_key.len == 0 && conf->amqp_ssl_client_certificate.len > 0 ) {
          ngx_log_error(NGX_LOG_EMERG, cf->log, 0,
                        "no amqp_ssl_client_certificate_key provided");
          return NGX_CONF_ERROR;
      }
    }
    
    return NGX_CONF_OK;
}
static void ngx_http_amqp_exit(ngx_cycle_t* cycle){
    ngx_http_amqp_conf_t* amcf=(ngx_http_amqp_conf_t*)ngx_get_conf(cycle->conf_ctx, ngx_http_amqp_module);
    amqp_channel_close(amcf->conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(amcf->conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(amcf->conn);
}