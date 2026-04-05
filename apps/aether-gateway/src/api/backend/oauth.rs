use axum::routing::{any, get};
use axum::Router;

use crate::{handlers::proxy::proxy_request, state::AppState};

pub(crate) fn mount_oauth_routes(router: Router<AppState>) -> Router<AppState> {
    router
        .route("/api/oauth/providers", get(proxy_request))
        .route("/api/oauth/{*oauth_public_path}", get(proxy_request))
        .route("/api/user/oauth/bindable-providers", get(proxy_request))
        .route("/api/user/oauth/links", get(proxy_request))
        .route("/api/user/oauth/{*oauth_user_path}", any(proxy_request))
}
