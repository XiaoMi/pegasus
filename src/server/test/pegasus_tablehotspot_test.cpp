#include "server/table_hotspot_policy.h"

#include <gtest/gtest.h>

namespace pegasus {
namespace server {

TEST(table_hotspot_policy, Algo1)
{
    std::vector<row_data> test_rows(2);
    row_data test_row1, test_row2;
    test_row1.get_qps = 1234.0;
    test_rows.at(0) = (test_row1);
    test_row2.get_qps = 4321.0;
    test_rows.at(1) = (test_row2);
    hotspot_calculator test_hotspot_calculator("TEST", 2);
    test_hotspot_calculator.aggregate(test_rows);
    test_hotspot_calculator.start_alg();
    std::vector<double> result;
    test_hotspot_calculator.get_hotpot_point_value(result);
    std::vector<double> expect_vector{1234.0, 4321.0};
    ASSERT_EQ((expect_vector == result), 1);
}
}
}
