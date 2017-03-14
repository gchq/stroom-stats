

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

package stroom.stats.main;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import stroom.stats.HBaseClient;

import java.util.Scanner;

//needs to extend ThreadScopeRunnable as the AOP marshaling of the StatisticDataSource (specifically the  MarshalOptions bean) needs thread scope
public class HbasePurgeRunner {
    public static void main(final String[] args) throws Exception {
        boolean proceed = false;

        Scanner consoleInScanner = null;

        try {
            consoleInScanner = new Scanner(System.in);

            System.out.println("This process will run the purge process, thus deleting lots of data!\n\n");

            System.out.println("Do you wish to continue? [yes|no]\n");

            while (consoleInScanner.hasNext()) {
                if (consoleInScanner.next().equals("yes")) {
                    proceed = true;

                }
                break;
            }
        } finally {
            if (consoleInScanner != null) {
                consoleInScanner.close();
            }

        }

        if (proceed) {
            final Injector injector = Guice.createInjector(new AbstractModule() {
                @Override
                protected void configure() {
                    bind(HBaseClient.class);
                }
            });

            final HBaseClient hBaseClient = injector.getInstance(HBaseClient.class);
            hBaseClient.purgeAllData();
            System.out.println("\nAll done!");

        } else {
            System.out.println("Exiting");
            System.exit(0);
        }
    }

}
