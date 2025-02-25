// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod role_mgr;
mod user;
mod user_api;
mod user_mgr;
mod user_stage;
mod user_udf;

pub mod auth;
pub mod role_cache_mgr;
mod user_setting;
mod user_warehouse;

pub use auth::auth_mgr::AuthMgr;
pub use auth::auth_mgr::Credential;
pub use role_cache_mgr::RoleCacheMgr;
pub use user::CertifiedInfo;
pub use user::User;
pub use user_api::UserApiProvider;
