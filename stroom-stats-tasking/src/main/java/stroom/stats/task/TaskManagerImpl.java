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

package stroom.stats.task;

import stroom.stats.task.api.Task;
import stroom.stats.task.api.TaskCallback;
import stroom.stats.task.api.TaskManager;
import stroom.stats.task.api.ThreadPool;

public class TaskManagerImpl implements TaskManager{
    @Override
    public <R> void execAsync(final Task<R> task) {
       throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public <R> void execAsync(final Task<R> task, final ThreadPool threadPool) {

        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public <R> void execAsync(final Task<R> task, final TaskCallback<R> callback, final ThreadPool threadPool) {
        throw new UnsupportedOperationException("Not yet implemented");

    }

    @Override
    public <R> void execAsync(final Task<R> task, final TaskCallback<R> callback) {
        throw new UnsupportedOperationException("Not yet implemented");

    }
}
