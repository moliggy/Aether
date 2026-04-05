#![allow(dead_code, unused_imports)]

pub(crate) use crate::provider_transport::vertex::{
    build_vertex_api_key_gemini_content_url, build_vertex_api_key_imagen_content_url,
    resolve_local_vertex_api_key_query_auth, supports_local_vertex_api_key_gemini_transport,
    supports_local_vertex_api_key_gemini_transport_with_network,
    supports_local_vertex_api_key_imagen_transport,
    supports_local_vertex_api_key_imagen_transport_with_network, VertexApiKeyQueryAuth,
    PROVIDER_TYPE, VERTEX_API_KEY_BASE_URL, VERTEX_API_KEY_QUERY_PARAM,
};
