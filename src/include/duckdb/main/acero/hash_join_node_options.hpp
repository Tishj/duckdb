#pragma once

#include "duckdb/main/acero/dataset/exec_node_options.hpp"
#include "duckdb/main/acero/dataset/dataset.hpp"
#include "duckdb/main/acero/dataset/scan_options.hpp"
#include "duckdb/main/acero/compute/expression.hpp"

namespace duckdb {
namespace ac {

enum class JoinType { LEFT_SEMI, RIGHT_SEMI, LEFT_ANTI, RIGHT_ANTI, INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER };

enum class JoinKeyCmp { EQ, IS };

/// \brief a node which implements a join operation using a hash table
class HashJoinNodeOptions : public arrow::dataset::ExecNodeOptions {
	using base = arrow::dataset::ExecNodeOptions;

public:
	static constexpr const char *default_output_suffix_for_left = "";
	static constexpr const char *default_output_suffix_for_right = "";
	/// \brief create an instance from values that outputs all columns
	HashJoinNodeOptions(JoinType in_join_type, std::vector<string> in_left_keys, std::vector<string> in_right_keys,
	                    cp::Expression filter = cp::literal(true),
	                    std::string output_suffix_for_left = default_output_suffix_for_left,
	                    std::string output_suffix_for_right = default_output_suffix_for_right,
	                    bool disable_bloom_filter = false)
	    : base(base::OptionType::HASH_JOIN_NODE), join_type(in_join_type), left_keys(std::move(in_left_keys)),
	      right_keys(std::move(in_right_keys)), output_all(true),
	      output_suffix_for_left(std::move(output_suffix_for_left)),
	      output_suffix_for_right(std::move(output_suffix_for_right)), filter(std::move(filter)),
	      disable_bloom_filter(disable_bloom_filter) {
		this->key_cmp.resize(this->left_keys.size());
		for (size_t i = 0; i < this->left_keys.size(); ++i) {
			this->key_cmp[i] = JoinKeyCmp::EQ;
		}
	}
	/// \brief create an instance from keys
	///
	/// This will create an inner join that outputs all columns and has no post join filter
	///
	/// `in_left_keys` should have the same length and types as `in_right_keys`
	/// @param in_left_keys the keys in the left input
	/// @param in_right_keys the keys in the right input
	HashJoinNodeOptions(std::vector<string> in_left_keys, std::vector<string> in_right_keys)
	    : base(base::OptionType::HASH_JOIN_NODE), left_keys(std::move(in_left_keys)),
	      right_keys(std::move(in_right_keys)) {
		this->join_type = JoinType::INNER;
		this->output_all = true;
		this->output_suffix_for_left = default_output_suffix_for_left;
		this->output_suffix_for_right = default_output_suffix_for_right;
		this->key_cmp.resize(this->left_keys.size());
		for (size_t i = 0; i < this->left_keys.size(); ++i) {
			this->key_cmp[i] = JoinKeyCmp::EQ;
		}
		this->filter = cp::literal(true);
	}
	/// \brief create an instance from values using JoinKeyCmp::EQ for all comparisons
	HashJoinNodeOptions(JoinType join_type, std::vector<string> left_keys, std::vector<string> right_keys,
	                    std::vector<string> left_output, std::vector<string> right_output,
	                    cp::Expression filter = cp::literal(true),
	                    std::string output_suffix_for_left = default_output_suffix_for_left,
	                    std::string output_suffix_for_right = default_output_suffix_for_right,
	                    bool disable_bloom_filter = false)
	    : base(base::OptionType::HASH_JOIN_NODE), join_type(join_type), left_keys(std::move(left_keys)),
	      right_keys(std::move(right_keys)), output_all(false), left_output(std::move(left_output)),
	      right_output(std::move(right_output)), output_suffix_for_left(std::move(output_suffix_for_left)),
	      output_suffix_for_right(std::move(output_suffix_for_right)), filter(std::move(filter)),
	      disable_bloom_filter(disable_bloom_filter) {
		this->key_cmp.resize(this->left_keys.size());
		for (size_t i = 0; i < this->left_keys.size(); ++i) {
			this->key_cmp[i] = JoinKeyCmp::EQ;
		}
	}
	/// \brief create an instance from values
	HashJoinNodeOptions(JoinType join_type, std::vector<string> left_keys, std::vector<string> right_keys,
	                    std::vector<string> left_output, std::vector<string> right_output,
	                    std::vector<JoinKeyCmp> key_cmp, cp::Expression filter = cp::literal(true),
	                    std::string output_suffix_for_left = default_output_suffix_for_left,
	                    std::string output_suffix_for_right = default_output_suffix_for_right,
	                    bool disable_bloom_filter = false)
	    : base(base::OptionType::HASH_JOIN_NODE), join_type(join_type), left_keys(std::move(left_keys)),
	      right_keys(std::move(right_keys)), output_all(false), left_output(std::move(left_output)),
	      right_output(std::move(right_output)), key_cmp(std::move(key_cmp)),
	      output_suffix_for_left(std::move(output_suffix_for_left)),
	      output_suffix_for_right(std::move(output_suffix_for_right)), filter(std::move(filter)),
	      disable_bloom_filter(disable_bloom_filter) {
	}

	HashJoinNodeOptions() : base(base::OptionType::HASH_JOIN_NODE) {
	}

	// type of join (inner, left, semi...)
	JoinType join_type = JoinType::INNER;
	// key fields from left input
	std::vector<string> left_keys;
	// key fields from right input
	std::vector<string> right_keys;
	// if set all valid fields from both left and right input will be output
	// (and field ref vectors for output fields will be ignored)
	bool output_all = false;
	// output fields passed from left input
	std::vector<string> left_output;
	// output fields passed from right input
	std::vector<string> right_output;
	// key comparison function (determines whether a null key is equal another null
	// key or not)
	std::vector<JoinKeyCmp> key_cmp;
	// suffix added to names of output fields coming from left input (used to distinguish,
	// if necessary, between fields of the same name in left and right input and can be left
	// empty if there are no name collisions)
	std::string output_suffix_for_left;
	// suffix added to names of output fields coming from right input
	std::string output_suffix_for_right;
	// residual filter which is applied to matching rows.  Rows that do not match
	// the filter are not included.  The filter is applied against the
	// concatenated input schema (left fields then right fields) and can reference
	// fields that are not included in the output.
	cp::Expression filter = cp::literal(true);
	// whether or not to disable Bloom filters in this join
	bool disable_bloom_filter = false;
};

} // namespace ac
} // namespace duckdb
