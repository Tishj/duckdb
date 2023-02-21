#include "http_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "s3fs.hpp"

namespace duckdb {

void HTTPFunctions::GetQueryParamFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	GenericExecutor::ExecuteBinary<PrimitiveType<string_t>, PrimitiveType<string_t>, PrimitiveType<string_t>>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](PrimitiveType<string_t> url, PrimitiveType<string_t> name) {
		    auto url_string = url.val.GetString();
		    auto name_string = name.val.GetString();

		    S3AuthParams s3_auth_params;
		    auto parsed_url = S3FileSystem::S3UrlParse(url_string, s3_auth_params);

		    duckdb_httplib_openssl::Params query_params;
		    duckdb_httplib_openssl::detail::parse_query_text(parsed_url.query_param, query_params);

		    string value;
		    if (!S3FileSystem::GetQueryParam(name_string, value, query_params)) {
			    return StringVector::AddString(result, nullptr, 0);
		    } else {
			    return StringVector::AddString(result, value);
		    }
	    });
}

} // namespace duckdb
