// Copyright 2016 Alex Regueiro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use libc::{self, c_char, c_void};

use std::ffi::{CStr, CString};
use std::path::Path;
use std::ptr;

use crate::Error;

pub fn error_message(ptr: *const c_char) -> String {
    let cstr = unsafe { CStr::from_ptr(ptr as *const _) };
    let s = String::from_utf8_lossy(cstr.to_bytes()).into_owned();
    unsafe {
        libc::free(ptr as *mut c_void);
    }
    s
}

pub fn opt_bytes_to_ptr<T: AsRef<[u8]>>(opt: Option<T>) -> *const c_char {
    match opt {
        Some(v) => v.as_ref().as_ptr() as *const c_char,
        None => ptr::null(),
    }
}

pub fn to_cpath<P, E>(path: P, error_message: E) -> Result<CString, Error>
where
    P: AsRef<Path>,
    E: AsRef<str>,
{
    match CString::new(path.as_ref().to_string_lossy().as_bytes()) {
        Ok(c) => Ok(c),
        Err(_) => Err(Error::new(error_message.as_ref().to_string())),
    }
}

pub fn to_cstring<S, E>(string: S, error_message: E) -> Result<CString, Error>
where
    S: AsRef<str>,
    E: AsRef<str>,
{
    match CString::new(string.as_ref().as_bytes()) {
        Ok(c) => Ok(c),
        Err(_) => Err(Error::new(error_message.as_ref().to_string())),
    }
}

macro_rules! ffi_try {
    ( $($function:ident)::*() ) => {
        ffi_try_impl!($($function)::*())
    };

    ( $($function:ident)::*( $arg1:expr $(, $arg:expr)* $(,)? ) ) => {
        ffi_try_impl!($($function)::*($arg1 $(, $arg)* ,))
    };
}

macro_rules! ffi_try_impl {
    ( $($function:ident)::*( $($arg:expr,)*) ) => {{
        let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
        let result = $($function)::*($($arg,)* &mut err);
        if !err.is_null() {
            return Err(Error::new($crate::ffi_util::error_message(err)));
        }
        result
    }};
}
