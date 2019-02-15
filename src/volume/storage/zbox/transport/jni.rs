use std::io::{Cursor, Read};
use std::mem;
use std::slice;

use http::{HeaderMap, Response as HttpResponse, Uri};
use jni::objects::{JObject, JValue};
use jni::{JNIEnv, JavaVM};

use super::{Response, Transport};
use error::{Error, Result};
use jni_lib::JVM;

// create URL parameter for JNI call
fn create_url_param<'a>(env: &JNIEnv<'a>, uri: &Uri) -> JValue<'a> {
    let url_str = env.new_string(&uri.to_string()).unwrap();
    let url_str = env.auto_local(*url_str);
    let url_obj = env
        .new_object(
            "java/net/URL",
            "(Ljava/lang/String;)V",
            &[JValue::Object(url_str.as_obj())],
        )
        .unwrap();
    JValue::Object(url_obj)
}

// create headers parameter for JNI call
fn create_headers_param<'a>(
    env: &JNIEnv<'a>,
    headers: &HeaderMap,
) -> JValue<'a> {
    let hdr_obj = env.new_object("java/util/HashMap", "()V", &[]).unwrap();

    for (name, value) in headers.iter() {
        let name_str = env.new_string(name.as_str()).unwrap();
        let value_str = env.new_string(value.to_str().unwrap()).unwrap();
        env.call_method(
            hdr_obj,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[JValue::Object(*name_str), JValue::Object(*value_str)],
        )
        .unwrap();

        env.delete_local_ref(*name_str).unwrap();
        env.delete_local_ref(*value_str).unwrap();
    }

    JValue::Object(hdr_obj)
}

// call request method on Java side
fn do_request<'a>(
    env: &'a JNIEnv<'a>,
    uri: &Uri,
    headers: &HeaderMap,
    method: &str,
    body: Option<JValue>,
) -> Result<JObject<'a>> {
    // create parameter list
    let param_url = create_url_param(&env, uri);
    let param_headers = create_headers_param(&env, headers);
    let mut params = vec![param_url, param_headers];
    if let Some(param_body) = body {
        params.push(param_body);
    };

    // request function signature
    let sig = if body.is_some() {
        "(Ljava/net/URL;Ljava/util/HashMap;[B)Lio/zbox/fs/transport/Response;"
    } else {
        "(Ljava/net/URL;Ljava/util/HashMap;)Lio/zbox/fs/transport/Response;"
    };

    // call request function on Java side
    let ret = match env.call_static_method(
        "io/zbox/fs/transport/HttpTransport",
        method,
        sig,
        &params,
    ) {
        Ok(resp_obj) => Ok(resp_obj.l().unwrap()),
        Err(err) => {
            // clear exception to prevent it from being thrown in Java side
            env.exception_clear().unwrap();
            Err(Error::from(err))
        }
    };

    // clear local reference
    env.delete_local_ref(param_url.l().unwrap()).unwrap();
    env.delete_local_ref(param_headers.l().unwrap()).unwrap();

    ret
}

// get response status code from HTTP request
#[inline]
fn get_response_status<'a>(env: &JNIEnv<'a>, resp_obj: JObject) -> i32 {
    env.get_field(resp_obj, "status", "I").unwrap().i().unwrap()
}

// make a response object from status code and body
fn create_response(status: i32, body: Vec<u8>) -> Result<Response> {
    let mut builder = HttpResponse::builder();
    builder.status(status as u16);
    let rdr = Cursor::new(body);
    let ret = Response::new(builder.body(Box::new(rdr) as Box<Read>)?);
    Ok(ret)
}

// transport using jni http layer
pub struct JniTransport {
    jvm: JavaVM,
}

impl JniTransport {
    pub fn new(timeout: u32) -> Result<Self> {
        let jvm = unsafe {
            let jvm = JVM.lock().unwrap();
            JavaVM::from_raw(jvm.get_java_vm_pointer())?
        };
        let ret = JniTransport { jvm };

        // initialise transport object in Java side
        {
            let env = ret.get_jni_env()?;
            env.call_static_method(
                "io/zbox/fs/transport/HttpTransport",
                "init",
                "(I)V",
                &[JValue::Int(timeout as i32)],
            )?;
        }

        Ok(ret)
    }

    #[inline]
    fn get_jni_env(&self) -> Result<JNIEnv> {
        self.jvm.get_env().map_err(Error::from)
    }
}

impl Transport for JniTransport {
    fn get(&self, uri: &Uri, headers: &HeaderMap) -> Result<Response> {
        let env = self.get_jni_env()?;

        // call get() on Java side
        let resp_obj = do_request(&env, uri, headers, "get", None)?;

        // if HTTP GET response is not succeed, return an empty body response
        let status = get_response_status(&env, resp_obj);
        if status != 200 {
            env.delete_local_ref(resp_obj).unwrap();
            return create_response(status, Vec::new());
        }

        // copy body bytes from Java to Rust
        let body = env.get_field(resp_obj, "body", "[B").unwrap().l().unwrap();
        let body_len =
            env.get_field(resp_obj, "len", "I").unwrap().i().unwrap();
        let mut buf = vec![0i8; body_len as usize];
        env.get_byte_array_region(body.into_inner(), 0, &mut buf[..])
            .unwrap();

        // clear local reference
        env.delete_local_ref(body).unwrap();
        env.delete_local_ref(resp_obj).unwrap();

        // convert Vec<i8> to Vec<u8>
        let buf: Vec<u8> = unsafe {
            let ptr = buf.as_mut_ptr();
            let len = buf.len();
            let cap = buf.capacity();
            mem::forget(buf);
            Vec::from_raw_parts(ptr as *mut u8, len, cap)
        };

        // create response
        create_response(status, buf)
    }

    fn put(
        &mut self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let env = self.get_jni_env()?;

        // convert Vec<u8> to Vec<i8>
        let buf: &[i8] = unsafe {
            slice::from_raw_parts(body.as_ptr() as *const i8, body.len())
        };

        // create byte array body
        let body = env.new_byte_array(body.len() as i32).unwrap();
        env.set_byte_array_region(body, 0, buf).unwrap();
        let body = JValue::Object(JObject::from(body));

        // call put() on Java side
        let resp_obj = do_request(&env, uri, headers, "put", Some(body))?;

        let status = get_response_status(&env, resp_obj);

        // clear local reference
        env.delete_local_ref(body.l().unwrap()).unwrap();
        env.delete_local_ref(resp_obj).unwrap();

        // create response
        create_response(status, Vec::new())
    }

    fn delete(&mut self, uri: &Uri, headers: &HeaderMap) -> Result<Response> {
        let env = self.get_jni_env()?;

        // call delete() on Java side
        let resp_obj = do_request(&env, uri, headers, "delete", None)?;
        let status = get_response_status(&env, resp_obj);

        // clear local reference
        env.delete_local_ref(resp_obj).unwrap();

        // create response
        create_response(status, Vec::new())
    }

    fn delete_bulk(
        &mut self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let env = self.get_jni_env()?;

        // convert Vec<u8> to Vec<i8>
        let buf: &[i8] = unsafe {
            slice::from_raw_parts(body.as_ptr() as *const i8, body.len())
        };

        // create byte array body
        let body = env.new_byte_array(buf.len() as i32).unwrap();
        env.set_byte_array_region(body, 0, buf).unwrap();
        let body = JValue::Object(JObject::from(body));

        // call delete_bulk() on Java side
        let resp_obj =
            do_request(&env, uri, headers, "delete_bulk", Some(body))?;
        let status = get_response_status(&env, resp_obj);

        // clear local reference
        env.delete_local_ref(resp_obj).unwrap();

        // create response
        create_response(status, Vec::new())
    }
}
