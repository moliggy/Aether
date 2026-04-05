use axum::routing::any;
use axum::Router;

use crate::{handlers::proxy::proxy_request, state::AppState};

pub(crate) fn mount_admin_routes(router: Router<AppState>) -> Router<AppState> {
    router.route("/api/admin/{*admin_path}", any(proxy_request))
}
