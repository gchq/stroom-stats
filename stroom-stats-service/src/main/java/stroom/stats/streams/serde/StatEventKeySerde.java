/*
 * Copyright 2017 Crown Copyright
 *
 * This file is part of Stroom-Stats.
 *
 * Stroom-Stats is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stroom-Stats is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stroom-Stats.  If not, see <http://www.gnu.org/licenses/>.
 */

package stroom.stats.streams.serde;

import org.apache.kafka.common.serialization.Serde;
import stroom.stats.streams.StatEventKey;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe // stateless serde
public class StatEventKeySerde {

    private StatEventKeySerde() {
    }

    public static Serde<StatEventKey> instance() {
        Serde<StatEventKey> statKeySerde = SerdeUtils.buildBasicSerde(
                (topic, obj) -> obj.getBytes(),
                (topic, bytes) -> StatEventKey.fromBytes(bytes)
        );
        return statKeySerde;
    }
}
