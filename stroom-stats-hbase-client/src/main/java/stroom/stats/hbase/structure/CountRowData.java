

/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.hbase.structure;

import java.util.Iterator;
import java.util.List;

public class CountRowData implements Iterable<CountCellIncrementHolder> {
    private final RowKey rowKey;
    private final List<CountCellIncrementHolder> cells;

    public CountRowData(final RowKey rowKey, final List<CountCellIncrementHolder> cells) {
        this.rowKey = rowKey;
        this.cells = cells;
    }

    public List<CountCellIncrementHolder> getCells() {
        return cells;
    }

    public RowKey getRowKey() {
        return rowKey;
    }

    @Override
    public Iterator<CountCellIncrementHolder> iterator() {
        return cells.iterator();
    }

    @Override
    public String toString() {
        return "CountRowData [rowKey=" + rowKey + ", cells=" + cells + "]";
    }
}
