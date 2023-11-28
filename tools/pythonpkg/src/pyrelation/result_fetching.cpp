#include "duckdb_python/pyrelation.hpp"

namespace duckdb {

unique_ptr<QueryResult> DuckDBPyRelation::ExecuteInternal(bool stream_result) {
	this->executed = true;
	return DuckDBPyRelation::ExecuteRelation(rel, stream_result);
}

void DuckDBPyRelation::ExecuteOrThrow(bool stream_result) {
	result.reset();
	auto query_result = ExecuteInternal(stream_result);
	if (!query_result) {
		throw InternalException("ExecuteOrThrow - no query available to execute");
	}
	if (query_result->HasError()) {
		query_result->ThrowError();
	}
	result = make_uniq<DuckDBPyResult>(std::move(query_result));
}

void DuckDBPyRelation::AssertResult() const {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
}

void DuckDBPyRelation::AssertResultOpen() const {
	if (!result || result->IsDepleted()) {
		throw InvalidInputException("No open result set");
	}
}

duckdb::pyarrow::RecordBatchReader DuckDBPyRelation::FetchRecordBatchReader(idx_t rows_per_batch) {
	AssertResult();
	return result->FetchRecordBatchReader(rows_per_batch);
}

PandasDataFrame DuckDBPyRelation::FetchDF(bool date_as_object) {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	if (result->IsDepleted()) {
		return py::none();
	}
	auto df = result->FetchDF(date_as_object);
	result = nullptr;
	return df;
}

Optional<py::tuple> DuckDBPyRelation::FetchOne() {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow(true);
	}
	if (result->IsDepleted()) {
		return py::none();
	}
	if (result && result->IsInvalidated()) {
		throw InvalidInputException(
		    "This relation is no longer valid, it can no longer be fetched from. Try re-executing the relation");
	}
	return result->Fetchone();
}

py::list DuckDBPyRelation::FetchMany(idx_t size) {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::list();
		}
		ExecuteOrThrow(true);
		D_ASSERT(result);
	}
	if (result->IsDepleted()) {
		return py::list();
	}
	if (result && result->IsInvalidated()) {
		throw InvalidInputException(
		    "This relation is no longer valid, it can no longer be fetched from. Try re-executing the relation");
	}
	return result->Fetchmany(size);
}

py::list DuckDBPyRelation::FetchAll() {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::list();
		}
		ExecuteOrThrow();
	}
	if (result->IsDepleted()) {
		return py::list();
	}
	if (result && result->IsInvalidated()) {
		throw InvalidInputException(
		    "This relation is no longer valid, it can no longer be fetched from. Try re-executing the relation");
	}
	auto res = result->Fetchall();
	result = nullptr;
	return res;
}

py::dict DuckDBPyRelation::FetchNumpy() {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	if (result->IsDepleted()) {
		return py::none();
	}
	if (result && result->IsInvalidated()) {
		throw InvalidInputException(
		    "This relation is no longer valid, it can no longer be fetched from. Try re-executing the relation");
	}
	auto res = result->FetchNumpy();
	result = nullptr;
	return res;
}

py::dict DuckDBPyRelation::FetchPyTorch() {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	if (result->IsDepleted()) {
		return py::none();
	}
	if (result && result->IsInvalidated()) {
		throw InvalidInputException(
		    "This relation is no longer valid, it can no longer be fetched from. Try re-executing the relation");
	}
	auto res = result->FetchPyTorch();
	result = nullptr;
	return res;
}

py::dict DuckDBPyRelation::FetchTF() {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	if (result->IsDepleted()) {
		return py::none();
	}
	if (result && result->IsInvalidated()) {
		throw InvalidInputException(
		    "This relation is no longer valid, it can no longer be fetched from. Try re-executing the relation");
	}
	auto res = result->FetchTF();
	result = nullptr;
	return res;
}

py::dict DuckDBPyRelation::FetchNumpyInternal(bool stream, idx_t vectors_per_chunk) {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	AssertResultOpen();
	auto res = result->FetchNumpyInternal(stream, vectors_per_chunk);
	result = nullptr;
	return res;
}

//! Should this also keep track of when the result is empty and set result->result_closed accordingly?
PandasDataFrame DuckDBPyRelation::FetchDFChunk(idx_t vectors_per_chunk, bool date_as_object) {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow(true);
	}
	if (result->IsDepleted()) {
		return result->FetchDFChunk(vectors_per_chunk, date_as_object);
	}
	if (result && result->IsInvalidated()) {
		throw InvalidInputException(
		    "This relation is no longer valid, it can no longer be fetched from. Try re-executing the relation");
	}
	AssertResultOpen();
	return result->FetchDFChunk(vectors_per_chunk, date_as_object);
}

duckdb::pyarrow::Table DuckDBPyRelation::ToArrowTable(idx_t batch_size) {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	AssertResultOpen();
	auto res = result->FetchArrowTable(batch_size);
	result = nullptr;
	return res;
}

PolarsDataFrame DuckDBPyRelation::ToPolars(idx_t batch_size) {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	auto arrow = ToArrowTable(batch_size);
	return py::cast<PolarsDataFrame>(pybind11::module_::import("polars").attr("DataFrame")(arrow));
}

duckdb::pyarrow::RecordBatchReader DuckDBPyRelation::ToRecordBatch(idx_t batch_size) {
	if (IsClosed()) {
		throw InvalidInputException("Relation has already been closed");
	}
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow(true);
	}
	AssertResultOpen();
	return result->FetchRecordBatchReader(batch_size);
}

} // namespace duckdb
