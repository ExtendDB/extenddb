// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use extenddb_core::types::{ProvisionedThroughput, ProvisionedThroughputDescription};

pub(crate) fn provisioned_throughput_description(
    throughput: &ProvisionedThroughput,
) -> ProvisionedThroughputDescription {
    ProvisionedThroughputDescription {
        read_capacity_units: throughput.read_capacity_units,
        write_capacity_units: throughput.write_capacity_units,
        number_of_decreases_today: 0,
        last_increase_date_time: None,
        last_decrease_date_time: None,
    }
}

pub(crate) fn provisioned_throughput_from_description(
    description: &ProvisionedThroughputDescription,
) -> ProvisionedThroughput {
    ProvisionedThroughput {
        read_capacity_units: description.read_capacity_units,
        write_capacity_units: description.write_capacity_units,
    }
}

pub(crate) fn zero_provisioned_throughput_description() -> ProvisionedThroughputDescription {
    ProvisionedThroughputDescription {
        read_capacity_units: 0,
        write_capacity_units: 0,
        number_of_decreases_today: 0,
        last_increase_date_time: None,
        last_decrease_date_time: None,
    }
}
