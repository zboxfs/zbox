#![allow(non_snake_case)]

use std::error::Error as StdError;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ptr::NonNull;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use jni::objects::{JByteBuffer, JClass, JObject, JString, JValue};
use jni::sys::{jboolean, jint, jlong, jobjectArray, JNI_FALSE};
use jni::{JNIEnv, JavaVM};

use base::crypto::{Cipher, MemLimit, OpsLimit};
use base::init_env;
use error::Result;
use file::{File, VersionReader};
use fs::{Metadata, Version};
use repo::{OpenOptions, Repo, RepoOpener};

// field name in Java class to hold its Rust object
const RUST_OBJ_FIELD: &'static str = "rustObj";

// field name in Java class to identify Rust object
// 100 - RepoOpener
// 101 - Repo
// 102 - OpenOptions
// 103 - File
// 104 - VersionReader
const RUST_OBJID_FIELD: &'static str = "rustObjId";

lazy_static! {
    // global JVM pointer
    pub static ref JVM: Mutex<JavaVM> = unsafe {
        let p = NonNull::dangling();
        Mutex::new(JavaVM::from_raw(p.as_ptr()).unwrap())
    };
}

#[inline]
fn u8_to_bool(a: u8) -> bool {
    match a {
        0 => false,
        1 => true,
        _ => unreachable!(),
    }
}

#[inline]
fn time_to_secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

#[inline]
fn check_version_limit(limit: jint) -> jint {
    if 1 <= limit && limit <= 255 {
        limit
    } else {
        0
    }
}

#[inline]
fn throw(env: &JNIEnv, err: &StdError) {
    let _ = env.throw_new("io/zbox/ZboxException", err.description());
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Env_init(
    env: JNIEnv,
    _class: JClass,
) -> jint {
    init_env();

    // save global JVM pointer
    let jvm = env.get_java_vm().unwrap();
    let mut jvm_ptr = JVM.lock().unwrap();
    *jvm_ptr = jvm;

    0
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RustObject_jniSetRustObj(
    env: JNIEnv,
    obj: JObject,
) {
    let cls = env.get_object_class(obj).unwrap();
    let cls = env.auto_local(*cls);
    let rust_obj_id =
        env.get_static_field(&cls, RUST_OBJID_FIELD, "I").unwrap();
    match rust_obj_id.i().unwrap() {
        100 => {
            let rust_obj = RepoOpener::new();
            unsafe {
                env.set_rust_field(obj, RUST_OBJ_FIELD, rust_obj).unwrap();
            }
        }
        102 => {
            let rust_obj = OpenOptions::new();
            unsafe {
                env.set_rust_field(obj, RUST_OBJ_FIELD, rust_obj).unwrap();
            }
        }
        _ => {}
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RustObject_jniTakeRustObj(
    env: JNIEnv,
    obj: JObject,
) {
    let cls = env.get_object_class(obj).unwrap();
    let cls = env.auto_local(*cls);
    let rust_obj_id =
        env.get_static_field(&cls, RUST_OBJID_FIELD, "I").unwrap();
    match rust_obj_id.i().unwrap() {
        100 => unsafe {
            env.take_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
                .unwrap();
        },
        101 => unsafe {
            env.take_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
                .unwrap();
        },
        102 => unsafe {
            env.take_rust_field::<&str, OpenOptions>(obj, RUST_OBJ_FIELD)
                .unwrap();
        },
        103 => unsafe {
            env.take_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
                .unwrap();
        },
        104 => unsafe {
            env.take_rust_field::<&str, VersionReader>(obj, RUST_OBJ_FIELD)
                .unwrap();
        },
        _ => {}
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RepoOpener_jniOpsLimit(
    env: JNIEnv,
    obj: JObject,
    limit: jint,
) {
    let mut opener = unsafe {
        env.get_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opener.ops_limit(OpsLimit::from(limit));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RepoOpener_jniMemLimit(
    env: JNIEnv,
    obj: JObject,
    limit: jint,
) {
    let mut opener = unsafe {
        env.get_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opener.mem_limit(MemLimit::from(limit));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RepoOpener_jniCipher(
    env: JNIEnv,
    obj: JObject,
    cipher: jint,
) {
    let mut opener = unsafe {
        env.get_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opener.cipher(Cipher::from(cipher));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RepoOpener_jniCreate(
    env: JNIEnv,
    obj: JObject,
    create: jboolean,
) {
    let mut opener = unsafe {
        env.get_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opener.create(u8_to_bool(create));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RepoOpener_jniCreateNew(
    env: JNIEnv,
    obj: JObject,
    create_new: jboolean,
) {
    let mut opener = unsafe {
        env.get_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opener.create_new(u8_to_bool(create_new));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RepoOpener_jniCompress(
    env: JNIEnv,
    obj: JObject,
    compress: jboolean,
) {
    let mut opener = unsafe {
        env.get_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opener.compress(u8_to_bool(compress));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RepoOpener_jniVersionLimit(
    env: JNIEnv,
    obj: JObject,
    limit: jint,
) {
    let mut opener = unsafe {
        env.get_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let limit = check_version_limit(limit);
    opener.version_limit(limit as u8);
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RepoOpener_jniDedupChunk(
    env: JNIEnv,
    obj: JObject,
    dedup: jboolean,
) {
    let mut opener = unsafe {
        env.get_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opener.dedup_chunk(u8_to_bool(dedup));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RepoOpener_jniReadOnly(
    env: JNIEnv,
    obj: JObject,
    read_only: jboolean,
) {
    let mut opener = unsafe {
        env.get_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opener.read_only(u8_to_bool(read_only));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_RepoOpener_jniOpen<'a>(
    env: JNIEnv<'a>,
    obj: JObject,
    uri: JString,
    pwd: JString,
) -> JObject<'a> {
    let opener = unsafe {
        env.get_rust_field::<&str, RepoOpener>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let uri: String = env.get_string(uri).unwrap().into();
    let pwd: String = env.get_string(pwd).unwrap().into();
    match opener.open(&uri, &pwd) {
        Ok(repo) => {
            let repo_obj = env.new_object("io/zbox/Repo", "()V", &[]).unwrap();
            unsafe {
                env.set_rust_field(repo_obj, RUST_OBJ_FIELD, repo).unwrap();
            }
            repo_obj
        }
        Err(ref err) => {
            throw(&env, err);
            JObject::null()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniExists(
    env: JNIEnv,
    _cls: JClass,
    uri: JString,
) -> jboolean {
    let uri: String = env.get_string(uri).unwrap().into();
    match Repo::exists(&uri) {
        Ok(ret) => ret as u8,
        Err(ref err) => {
            throw(&env, err);
            JNI_FALSE
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniClose(env: JNIEnv, obj: JObject) {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };

    match repo.close() {
        Ok(_) => {}
        Err(ref err) => {
            throw(&env, err);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniInfo<'a>(
    env: JNIEnv<'a>,
    obj: JObject,
) -> JObject<'a> {
    let repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };

    let info = repo.info();
    if let Err(ref err) = info {
        throw(&env, err);
        return JObject::null();
    }
    let info = info.unwrap();

    let info_obj = env.new_object("io/zbox/RepoInfo", "()V", &[]).unwrap();

    let vol_id = env
        .byte_array_from_slice(info.volume_id().as_ref())
        .unwrap();
    let ver = env.new_string(info.version()).unwrap();
    let uri = env.new_string(info.uri()).unwrap();

    let ops_str = format!("{:?}", info.ops_limit()).to_uppercase();
    let ops_limit = env.new_string(ops_str).unwrap();
    let ops_obj = env
        .call_static_method(
            "io/zbox/OpsLimit",
            "valueOf",
            "(Ljava/lang/String;)Lio/zbox/OpsLimit;",
            &[JValue::Object(*ops_limit)],
        ).unwrap();

    let mem_str = format!("{:?}", info.mem_limit()).to_uppercase();
    let mem_limit = env.new_string(mem_str).unwrap();
    let mem_obj = env
        .call_static_method(
            "io/zbox/MemLimit",
            "valueOf",
            "(Ljava/lang/String;)Lio/zbox/MemLimit;",
            &[JValue::Object(*mem_limit)],
        ).unwrap();

    let cipher_str = format!("{:?}", info.cipher()).to_uppercase();
    let cipher = env.new_string(cipher_str).unwrap();
    let cipher_obj = env
        .call_static_method(
            "io/zbox/Cipher",
            "valueOf",
            "(Ljava/lang/String;)Lio/zbox/Cipher;",
            &[JValue::Object(*cipher)],
        ).unwrap();

    env.set_field(
        info_obj,
        "volumeId",
        "[B",
        JValue::Object(JObject::from(vol_id)),
    ).unwrap();
    env.set_field(
        info_obj,
        "version",
        "Ljava/lang/String;",
        JValue::Object(JObject::from(ver)),
    ).unwrap();
    env.set_field(
        info_obj,
        "uri",
        "Ljava/lang/String;",
        JValue::Object(JObject::from(uri)),
    ).unwrap();
    env.set_field(info_obj, "opsLimit", "Lio/zbox/OpsLimit;", ops_obj)
        .unwrap();
    env.set_field(info_obj, "memLimit", "Lio/zbox/MemLimit;", mem_obj)
        .unwrap();
    env.set_field(info_obj, "cipher", "Lio/zbox/Cipher;", cipher_obj)
        .unwrap();
    env.set_field(
        info_obj,
        "compress",
        "Z",
        JValue::Bool(info.compress() as u8),
    ).unwrap();
    env.set_field(
        info_obj,
        "versionLimit",
        "I",
        JValue::Int(info.version_limit() as i32),
    ).unwrap();
    env.set_field(
        info_obj,
        "dedupChunk",
        "Z",
        JValue::Bool(info.dedup_chunk() as u8),
    ).unwrap();
    env.set_field(
        info_obj,
        "isReadOnly",
        "Z",
        JValue::Bool(info.is_read_only() as u8),
    ).unwrap();
    env.set_field(
        info_obj,
        "createdAt",
        "J",
        JValue::Long(time_to_secs(info.created_at())),
    ).unwrap();

    info_obj
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniResetPassword(
    env: JNIEnv,
    obj: JObject,
    old_pwd: JString,
    new_pwd: JString,
    ops_limit: jint,
    mem_limit: jint,
) {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let old_pwd: String = env.get_string(old_pwd).unwrap().into();
    let new_pwd: String = env.get_string(new_pwd).unwrap().into();
    if let Err(ref err) = repo.reset_password(
        &old_pwd,
        &new_pwd,
        OpsLimit::from(ops_limit),
        MemLimit::from(mem_limit),
    ) {
        throw(&env, err);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniPathExists(
    env: JNIEnv,
    obj: JObject,
    path: JString,
) -> jboolean {
    let repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    match repo.path_exists(&path) {
        Ok(result) => result as u8,
        Err(ref err) => {
            throw(&env, err);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniIsFile(
    env: JNIEnv,
    obj: JObject,
    path: JString,
) -> jboolean {
    let repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    match repo.is_file(&path) {
        Ok(result) => result as u8,
        Err(ref err) => {
            throw(&env, err);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniIsDir(
    env: JNIEnv,
    obj: JObject,
    path: JString,
) -> jboolean {
    let repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    match repo.is_dir(&path) {
        Ok(result) => result as u8,
        Err(ref err) => {
            throw(&env, err);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniCreateFile<'a>(
    env: JNIEnv<'a>,
    obj: JObject,
    path: JString,
) -> JObject<'a> {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };

    let path: String = env.get_string(path).unwrap().into();
    match repo.create_file(&path) {
        Ok(file) => {
            let file_obj = env.new_object("io/zbox/File", "()V", &[]).unwrap();
            unsafe {
                env.set_rust_field(file_obj, RUST_OBJ_FIELD, file).unwrap();
            }
            file_obj
        }
        Err(ref err) => {
            throw(&env, err);
            JObject::null()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniOpenFile<'a>(
    env: JNIEnv<'a>,
    obj: JObject,
    path: JString,
) -> JObject<'a> {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    match repo.open_file(&path) {
        Ok(file) => {
            let file_obj = env.new_object("io/zbox/File", "()V", &[]).unwrap();
            unsafe {
                env.set_rust_field(file_obj, RUST_OBJ_FIELD, file).unwrap();
            }
            file_obj
        }
        Err(ref err) => {
            throw(&env, err);
            JObject::null()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniCreateDir(
    env: JNIEnv,
    obj: JObject,
    path: JString,
) {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    if let Err(ref err) = repo.create_dir(&path) {
        throw(&env, err);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniCreateDirAll(
    env: JNIEnv,
    obj: JObject,
    path: JString,
) {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    if let Err(ref err) = repo.create_dir_all(&path) {
        throw(&env, err);
    }
}

fn metadata_to_jobject<'a>(env: &JNIEnv<'a>, meta: Metadata) -> JObject<'a> {
    let meta_obj = env.new_object("io/zbox/Metadata", "()V", &[]).unwrap();

    let ftype_str = format!("{:?}", meta.file_type()).to_uppercase();
    let ftype = env.new_string(ftype_str).unwrap();
    let ftype_obj = env
        .call_static_method(
            "io/zbox/FileType",
            "valueOf",
            "(Ljava/lang/String;)Lio/zbox/FileType;",
            &[JValue::Object(*ftype)],
        ).unwrap();

    env.set_field(meta_obj, "fileType", "Lio/zbox/FileType;", ftype_obj)
        .unwrap();
    env.set_field(meta_obj, "len", "J", JValue::Long(meta.len() as i64))
        .unwrap();
    env.set_field(
        meta_obj,
        "currVersion",
        "I",
        JValue::Int(meta.curr_version() as i32),
    ).unwrap();
    env.set_field(
        meta_obj,
        "createdAt",
        "J",
        JValue::Long(time_to_secs(meta.created_at())),
    ).unwrap();
    env.set_field(
        meta_obj,
        "modifiedAt",
        "J",
        JValue::Long(time_to_secs(meta.modified_at())),
    ).unwrap();

    env.delete_local_ref(*ftype).unwrap();
    env.delete_local_ref(ftype_obj.l().unwrap()).unwrap();

    meta_obj
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniReadDir(
    env: JNIEnv,
    obj: JObject,
    path: JString,
) -> jobjectArray {
    let repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    match repo.read_dir(&path) {
        Ok(ents) => {
            let objs = env
                .new_object_array(
                    ents.len() as i32,
                    "io/zbox/DirEntry",
                    JObject::null(),
                ).unwrap();

            for (i, ent) in ents.iter().enumerate() {
                let ent_obj =
                    env.new_object("io/zbox/DirEntry", "()V", &[]).unwrap();
                let path_str =
                    env.new_string(ent.path().to_str().unwrap()).unwrap();
                let name_str = env.new_string(ent.file_name()).unwrap();
                let meta_obj = metadata_to_jobject(&env, ent.metadata());

                env.set_field(
                    ent_obj,
                    "path",
                    "Ljava/lang/String;",
                    JValue::Object(JObject::from(path_str)),
                ).unwrap();
                env.set_field(
                    ent_obj,
                    "fileName",
                    "Ljava/lang/String;",
                    JValue::Object(JObject::from(name_str)),
                ).unwrap();
                env.set_field(
                    ent_obj,
                    "metadata",
                    "Lio/zbox/Metadata;",
                    JValue::Object(JObject::from(meta_obj)),
                ).unwrap();

                env.set_object_array_element(objs, i as i32, ent_obj)
                    .unwrap();

                env.delete_local_ref(ent_obj).unwrap();
                env.delete_local_ref(*path_str).unwrap();
                env.delete_local_ref(*name_str).unwrap();
                env.delete_local_ref(meta_obj).unwrap();
            }

            objs
        }
        Err(ref err) => {
            throw(&env, err);
            env.new_object_array(0, "io/zbox/DirEntry", JObject::null())
                .unwrap()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniMetadata<'a>(
    env: JNIEnv<'a>,
    obj: JObject,
    path: JString,
) -> JObject<'a> {
    let repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    match repo.metadata(&path) {
        Ok(meta) => metadata_to_jobject(&env, meta),
        Err(ref err) => {
            throw(&env, err);
            JObject::null()
        }
    }
}

fn versions_to_jobjects(
    env: &JNIEnv,
    history: Result<Vec<Version>>,
) -> jobjectArray {
    match history {
        Ok(vers) => {
            let objs = env
                .new_object_array(
                    vers.len() as i32,
                    "io/zbox/Version",
                    JObject::null(),
                ).unwrap();

            for (i, ver) in vers.iter().enumerate() {
                let ver_obj =
                    env.new_object("io/zbox/Version", "()V", &[]).unwrap();

                env.set_field(
                    ver_obj,
                    "num",
                    "J",
                    JValue::Long(ver.num() as i64),
                ).unwrap();
                env.set_field(
                    ver_obj,
                    "len",
                    "J",
                    JValue::Long(ver.len() as i64),
                ).unwrap();
                env.set_field(
                    ver_obj,
                    "createdAt",
                    "J",
                    JValue::Long(time_to_secs(ver.created_at())),
                ).unwrap();

                env.set_object_array_element(objs, i as i32, ver_obj)
                    .unwrap();

                env.delete_local_ref(ver_obj).unwrap();
            }

            objs
        }
        Err(ref err) => {
            throw(&env, err);
            env.new_object_array(0, "io/zbox/Version", JObject::null())
                .unwrap()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniHistory(
    env: JNIEnv,
    obj: JObject,
    path: JString,
) -> jobjectArray {
    let repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    versions_to_jobjects(&env, repo.history(&path))
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniCopy(
    env: JNIEnv,
    obj: JObject,
    from: JString,
    to: JString,
) {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let from: String = env.get_string(from).unwrap().into();
    let to: String = env.get_string(to).unwrap().into();
    if let Err(ref err) = repo.copy(&from, &to) {
        throw(&env, err);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniRemoveFile(
    env: JNIEnv,
    obj: JObject,
    path: JString,
) {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    if let Err(ref err) = repo.remove_file(&path) {
        throw(&env, err);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniRemoveDir(
    env: JNIEnv,
    obj: JObject,
    path: JString,
) {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    if let Err(ref err) = repo.remove_dir(&path) {
        throw(&env, err);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniRemoveDirAll(
    env: JNIEnv,
    obj: JObject,
    path: JString,
) {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    if let Err(ref err) = repo.remove_dir_all(&path) {
        throw(&env, err);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_Repo_jniRename(
    env: JNIEnv,
    obj: JObject,
    from: JString,
    to: JString,
) {
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let from: String = env.get_string(from).unwrap().into();
    let to: String = env.get_string(to).unwrap().into();
    if let Err(ref err) = repo.rename(&from, &to) {
        throw(&env, err);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_OpenOptions_jniRead(
    env: JNIEnv,
    obj: JObject,
    read: jboolean,
) {
    let mut opts = unsafe {
        env.get_rust_field::<&str, OpenOptions>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opts.read(u8_to_bool(read));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_OpenOptions_jniWrite(
    env: JNIEnv,
    obj: JObject,
    write: jboolean,
) {
    let mut opts = unsafe {
        env.get_rust_field::<&str, OpenOptions>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opts.write(u8_to_bool(write));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_OpenOptions_jniAppend(
    env: JNIEnv,
    obj: JObject,
    append: jboolean,
) {
    let mut opts = unsafe {
        env.get_rust_field::<&str, OpenOptions>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opts.append(u8_to_bool(append));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_OpenOptions_jniTruncate(
    env: JNIEnv,
    obj: JObject,
    truncate: jboolean,
) {
    let mut opts = unsafe {
        env.get_rust_field::<&str, OpenOptions>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opts.truncate(u8_to_bool(truncate));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_OpenOptions_jniCreate(
    env: JNIEnv,
    obj: JObject,
    create: jboolean,
) {
    let mut opts = unsafe {
        env.get_rust_field::<&str, OpenOptions>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opts.create(u8_to_bool(create));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_OpenOptions_jniCreateNew(
    env: JNIEnv,
    obj: JObject,
    create_new: jboolean,
) {
    let mut opts = unsafe {
        env.get_rust_field::<&str, OpenOptions>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opts.create_new(u8_to_bool(create_new));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_OpenOptions_jniVersionLimit(
    env: JNIEnv,
    obj: JObject,
    limit: jint,
) {
    let mut opts = unsafe {
        env.get_rust_field::<&str, OpenOptions>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let limit = check_version_limit(limit);
    opts.version_limit(limit as u8);
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_OpenOptions_jniDedupChunk(
    env: JNIEnv,
    obj: JObject,
    dedup: jboolean,
) {
    let mut opts = unsafe {
        env.get_rust_field::<&str, OpenOptions>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    opts.dedup_chunk(u8_to_bool(dedup));
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_OpenOptions_jniOpen<'a>(
    env: JNIEnv<'a>,
    obj: JObject,
    repo: JObject,
    path: JString,
) -> JObject<'a> {
    let opts = unsafe {
        env.get_rust_field::<&str, OpenOptions>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let mut repo = unsafe {
        env.get_rust_field::<&str, Repo>(repo, RUST_OBJ_FIELD)
            .unwrap()
    };
    let path: String = env.get_string(path).unwrap().into();
    match opts.open(&mut repo, &path) {
        Ok(file) => {
            let file_obj = env.new_object("io/zbox/File", "()V", &[]).unwrap();
            unsafe {
                env.set_rust_field(file_obj, RUST_OBJ_FIELD, file).unwrap();
            }
            file_obj
        }
        Err(ref err) => {
            throw(&env, err);
            JObject::null()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_File_jniMetadata<'a>(
    env: JNIEnv<'a>,
    obj: JObject,
) -> JObject<'a> {
    let file = unsafe {
        env.get_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    match file.metadata() {
        Ok(meta) => metadata_to_jobject(&env, meta),
        Err(ref err) => {
            throw(&env, err);
            JObject::null()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_File_jniHistory(
    env: JNIEnv,
    obj: JObject,
) -> jobjectArray {
    let file = unsafe {
        env.get_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    versions_to_jobjects(&env, file.history())
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_File_jniCurrVersion(
    env: JNIEnv,
    obj: JObject,
) -> jlong {
    let file = unsafe {
        env.get_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    match file.curr_version() {
        Ok(ver) => ver as i64,
        Err(ref err) => {
            throw(&env, err);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_File_jniVersionReader<'a, 'b>(
    env: JNIEnv<'a>,
    obj: JObject,
    ver_num: jlong,
) -> JObject<'a> {
    let file = unsafe {
        env.get_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    match file.version_reader(ver_num as usize) {
        Ok(rdr) => {
            let rdr_obj =
                env.new_object("io/zbox/VersionReader", "()V", &[]).unwrap();
            unsafe {
                env.set_rust_field(rdr_obj, RUST_OBJ_FIELD, rdr).unwrap();
            }
            rdr_obj
        }
        Err(ref err) => {
            throw(&env, err);
            JObject::null()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_File_jniFinish(env: JNIEnv, obj: JObject) {
    let mut file = unsafe {
        env.get_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    if let Err(ref err) = file.finish() {
        throw(&env, err);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_File_jniWriteOnce(
    env: JNIEnv,
    obj: JObject,
    buf: JByteBuffer,
) {
    let mut file = unsafe {
        env.get_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let buf = env.get_direct_buffer_address(buf).unwrap();
    if let Err(ref err) = file.write_once(buf) {
        throw(&env, err);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_File_jniSetLen(
    env: JNIEnv,
    obj: JObject,
    len: jlong,
) {
    let mut file = unsafe {
        env.get_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    if let Err(ref err) = file.set_len(len as usize) {
        throw(&env, err);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_File_jniRead(
    env: JNIEnv,
    obj: JObject,
    dst: JByteBuffer,
) -> jlong {
    let mut file = unsafe {
        env.get_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let dst = env.get_direct_buffer_address(dst).unwrap();
    match file.read(dst) {
        Ok(read) => read as i64,
        Err(ref err) => {
            throw(&env, err);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_File_jniWrite(
    env: JNIEnv,
    obj: JObject,
    buf: JByteBuffer,
) -> jlong {
    let mut file = unsafe {
        env.get_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let buf = env.get_direct_buffer_address(buf).unwrap();
    match file.write(buf) {
        Ok(written) => written as i64,
        Err(ref err) => {
            throw(&env, err);
            0
        }
    }
}

#[inline]
fn to_seek_from(offset: i64, whence: jint) -> SeekFrom {
    match whence {
        0 => SeekFrom::Start(offset as u64),
        1 => SeekFrom::Current(offset),
        2 => SeekFrom::End(offset),
        _ => unimplemented!(),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_File_jniSeek(
    env: JNIEnv,
    obj: JObject,
    offset: jlong,
    whence: jint,
) -> jlong {
    let mut file = unsafe {
        env.get_rust_field::<&str, File>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let whence = to_seek_from(offset, whence);
    match file.seek(whence) {
        Ok(pos) => pos as i64,
        Err(ref err) => {
            throw(&env, err);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_VersionReader_jniRead(
    env: JNIEnv,
    obj: JObject,
    dst: JByteBuffer,
) -> jlong {
    let mut rdr = unsafe {
        env.get_rust_field::<&str, VersionReader>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let dst = env.get_direct_buffer_address(dst).unwrap();
    match rdr.read(dst) {
        Ok(read) => read as i64,
        Err(ref err) => {
            throw(&env, err);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_zbox_VersionReader_jniSeek(
    env: JNIEnv,
    obj: JObject,
    offset: jlong,
    whence: jint,
) -> jlong {
    let mut rdr = unsafe {
        env.get_rust_field::<&str, VersionReader>(obj, RUST_OBJ_FIELD)
            .unwrap()
    };
    let whence = to_seek_from(offset, whence);
    match rdr.seek(whence) {
        Ok(pos) => pos as i64,
        Err(ref err) => {
            throw(&env, err);
            0
        }
    }
}
