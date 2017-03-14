

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

package stroom.stats.util.logging;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Wrapper around SLF4J to do lazy message generation using lambdas
 *
 * Also if you have noisy logging (e.g. "Written 2/100") you can use
 * xxxInterval(...) method to only log out every second.
 *
 * All slf4j.Logger methods are delegated to the slf4j.Logger implementation
 */
public final class LambdaLogger implements Logger {
    private final Logger logger;

    private long interval = 0;
    private long nextLogTime = 0;

    // Use a private constructor as this is only made via the static factory.
    private LambdaLogger(final Logger logger) {
        Preconditions.checkNotNull(logger);
        this.logger = logger;
    }

    public static final LambdaLogger getLogger(final Class<?> clazz) {
        final Logger logger = LoggerFactory.getLogger(clazz.getName());
        final LambdaLogger lambdaLogger = new LambdaLogger(logger);
        lambdaLogger.interval = 1000;
        return lambdaLogger;
    }

    public boolean checkInterval() {
        if (interval == 0) {
            return true;
        }
        // We don't care about thread race conditions ...
        final long time = System.currentTimeMillis();
        if (time > nextLogTime) {
            nextLogTime = time + interval;
            return true;
        }
        return false;

    }

    public void setInterval(final long interval) {
        this.interval = interval;
    }

    public void debugInterval(final String msg, final Object... args) {
        if (logger.isDebugEnabled() && checkInterval()) {
            logger.debug(msg,args);
        }
    }

    public void debugInterval(final Supplier<String> msgSupplier) {
        if (logger.isDebugEnabled() && checkInterval()) {
            logger.debug(getMessage(msgSupplier));
        }
    }

    public void traceInterval(final String msg, final Object... args) {
        if (logger.isTraceEnabled() && checkInterval()) {
            logger.trace(msg,args);
        }
    }

    public void traceInterval(final Supplier<String> msgSupplier) {
        if (logger.isTraceEnabled() && checkInterval()) {
            logger.trace(getMessage(msgSupplier));
        }
    }

    public void infoInterval(final String msg, final Object... args) {
        if (logger.isInfoEnabled() && checkInterval()) {
            logger.info(msg,args);
        }
    }

    public void infoInterval(final Supplier<String> msgSupplier) {
        if (logger.isInfoEnabled() && checkInterval()) {
            logger.info(getMessage(msgSupplier));
        }
    }


    /**
     * Logs a TRACE level message to the logger.
     *
     * @param msgSupplier
     *            A function that will supply the log message string
     * @param e
     *            The {@link Throwable} to attach to the message
     */
    public void trace(final Supplier<String> msgSupplier, final Throwable e) {
        if (logger.isTraceEnabled()) {
            logger.trace(getMessage(msgSupplier),e);
        }
    }

    /**
     * Logs a TRACE level message to the logger.
     *
     * @param msgSupplier
     *            A function that will supply the log message string
     */
    public void trace(final Supplier<String> msgSupplier) {
        if (logger.isTraceEnabled()) {
            logger.trace(getMessage(msgSupplier));
        }
    }

    /**
     * Logs a DEBUG level message to the logger.
     *
     * @param msgSupplier
     *            A function that will supply the log message string
     * @param e
     *            The {@link Throwable} to attach to the message
     */
    public void debug(final Supplier<String> msgSupplier, final Throwable e) {
        if (logger.isDebugEnabled()) {
            logger.debug(getMessage(msgSupplier),e);
        }
    }

    /**
     * Logs a DEBUG level message to the logger.
     *
     * @param msgSupplier
     *            A function that will supply the log message string
     */
    public void debug(final Supplier<String> msgSupplier) {
        if (logger.isDebugEnabled()) {
            logger.debug(getMessage(msgSupplier));
        }
    }


    /**
     * Logs a INFO level message to the logger.
     *
     * @param msgSupplier
     *            A function that will supply the log message string
     * @param e
     *            The {@link Throwable} to attach to the message
     */
    public void info(final Supplier<String> msgSupplier, final Throwable e) {
        if (logger.isInfoEnabled()) {
            logger.info(getMessage(msgSupplier),e);
        }
    }

    /**
     * Logs a INFO level message to the logger.
     *
     * @param msgSupplier
     *            A function that will supply the log message string
     */
    public void info(final Supplier<String> msgSupplier) {
        if (logger.isInfoEnabled()) {
            logger.info(getMessage(msgSupplier));
        }
    }

    /**
     * Logs a WARN level message to the logger.
     *
     * @param msgSupplier
     *            A function that will supply the log message string
     * @param e
     *            The {@link Throwable} to attach to the message
     */
    public void warn(final Supplier<String> msgSupplier, final Throwable e) {
        if (logger.isWarnEnabled()) {
            logger.warn(getMessage(msgSupplier), e);
        }
    }

    /**
     * Logs a WARN level message to the logger.
     *
     * @param msgSupplier
     *            A function that will supply the log message string
     */
    public void warn(final Supplier<String> msgSupplier) {
        if (logger.isWarnEnabled()) {
            logger.warn(getMessage(msgSupplier));
        }
    }

    /**
     * Logs a ERROR level message to the logger.
     *
     * @param msgSupplier
     *            A function that will supply the log message string
     * @param e
     *            The {@link Throwable} to attach to the message
     */
    public void error(final Supplier<String> msgSupplier, final Throwable e) {
        if (logger.isErrorEnabled()) {
            logger.error(getMessage(msgSupplier), e);
        }
    }

    /**
     * Logs a ERROR level message to the logger.
     *
     * @param msgSupplier
     *            A function that will supply the log message string
     */
    public void error(final Supplier<String> msgSupplier) {
        if (logger.isErrorEnabled()) {
            logger.error(getMessage(msgSupplier));
        }
    }

    @FunctionalInterface
    public interface Action {
        void doWork();
    }

    public enum ExceptionBehaviour {
        //let any exceptions in the action propagate out of the method
        THROW,
        //catch and log any exceptions in the action
        CATCH;
    }


    private void doConditionalAction(final BooleanSupplier condition, final Action action, ExceptionBehaviour exceptionBehaviour) {
        if (condition.getAsBoolean()) {
            switch (exceptionBehaviour) {
                case CATCH:
                    try {
                        action.doWork();
                    } catch (Exception e) {
                        logger.error("Error performing action", e);
                    }
                    break;
                case THROW:
                    action.doWork();
                    break;
            }
        }
    }

    /**
     * Runs the passed functionalInterface if trace is enabled.
     *
     * @param action
     *            The functionalInterface to run
     * @param exceptionBehaviour
     *              Whether to catch any exceptions in the action or let them propagate
     */
    public void ifTraceIsEnabled(final Action action, ExceptionBehaviour exceptionBehaviour) {
        doConditionalAction(logger::isTraceEnabled, action, exceptionBehaviour);
    }

    /**
     * Runs the passed functionalInterface if trace is enabled.
     * Any exceptions in the action will be caught and logged as errors
     *
     * @param action
     *            The functionalInterface to run
     */
    public void ifTraceIsEnabled(final Action action) {
        doConditionalAction(logger::isTraceEnabled, action, ExceptionBehaviour.CATCH);
    }

    /**
     * Runs the passed functionalInterface if debug is enabled.
     *
     * @param action
     *            The functionalInterface to run
     * @param exceptionBehaviour
     *              Whether to catch any exceptions in the action or let them propagate
     */
    public void ifDebugEnabled(final Action action, ExceptionBehaviour exceptionBehaviour) {
        doConditionalAction(logger::isDebugEnabled, action, exceptionBehaviour);
    }

    /**
     * Runs the passed functionalInterface if debug is enabled.
     * Any exceptions in the action will be caught and logged as errors
     *
     * @param action
     *            The functionalInterface to run
     */
    public void ifDebugIsEnabled(final Action action) {
        doConditionalAction(logger::isDebugEnabled, action, ExceptionBehaviour.CATCH);
    }


    /**
     * @return The underlying SLF4J Logger for non-lambda calls
     */
    public Logger getLogger() {
        return logger;
    }

    private String getMessage(final Supplier<String> msg) {
        try {
            return msg.get();
        } catch (Exception e) {
            //Catch the exception to prevent a logging bug from stopping the application
            logger.error("Error while trying to execute msgSupplier, exception swallowed", e);
            return "Error while trying to execute msgSupplier. See ERROR msg in the logs";
        }
    }

    /**
     * Return the name of this <code>Logger</code> instance.
     * @return name of this logger instance
     */
    public String getName() {
        return logger.getName();
    }

    /**
     * Is the logger instance enabled for the TRACE level?
     *
     * @return True if this Logger is enabled for the TRACE level,
     *         false otherwise.
     * @since 1.4
     */
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    /**
     * Log a message at the TRACE level.
     *
     * @param msg the message string to be logged
     * @since 1.4
     */
    public void trace(String msg) {
        logger.trace(msg);
    }

    /**
     * Log a message at the TRACE level according to the specified format
     * and argument.
     * <p/>
     * <p>This form avoids superfluous object creation when the logger
     * is disabled for the TRACE level. </p>
     *
     * @param format the format string
     * @param arg    the argument
     * @since 1.4
     */
    public void trace(String format, Object arg) {
        logger.trace(format, arg);
    }

    /**
     * Log a message at the TRACE level according to the specified format
     * and arguments.
     * <p/>
     * <p>This form avoids superfluous object creation when the logger
     * is disabled for the TRACE level. </p>
     *
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     * @since 1.4
     */
    public void trace(String format, Object arg1, Object arg2) {
        logger.trace(format, arg1, arg2);
    }

    /**
     * Log a message at the TRACE level according to the specified format
     * and arguments.
     * <p/>
     * <p>This form avoids superfluous string concatenation when the logger
     * is disabled for the TRACE level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
     * even if this logger is disabled for TRACE. The variants taking {@link #trace(String, Object) one} and
     * {@link #trace(String, Object, Object) two} arguments exist solely in order to avoid this hidden cost.</p>
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     * @since 1.4
     */
    public void trace(String format, Object... arguments) {
        logger.trace(format, arguments);
    }

    /**
     * Log an exception (throwable) at the TRACE level with an
     * accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     * @since 1.4
     */
    public void trace(String msg, Throwable t) {
        logger.trace(msg, t);
    }

    /**
     * Similar to {@link #isTraceEnabled()} method except that the
     * marker data is also taken into account.
     *
     * @param marker The marker data to take into consideration
     * @return True if this Logger is enabled for the TRACE level,
     *         false otherwise.
     *
     * @since 1.4
     */
    public boolean isTraceEnabled(Marker marker) {
        return logger.isTraceEnabled(marker);
    }

    /**
     * Log a message with the specific Marker at the TRACE level.
     *
     * @param marker the marker data specific to this log statement
     * @param msg    the message string to be logged
     * @since 1.4
     */
    public void trace(Marker marker, String msg) {
        logger.trace(marker, msg);
    }

    /**
     * This method is similar to {@link #trace(String, Object)} method except that the
     * marker data is also taken into consideration.
     *
     * @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg    the argument
     * @since 1.4
     */
    public void trace(Marker marker, String format, Object arg) {
        logger.trace(marker, format, arg);
    }

    /**
     * This method is similar to {@link #trace(String, Object, Object)}
     * method except that the marker data is also taken into
     * consideration.
     *
     * @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     * @since 1.4
     */
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        logger.trace(marker, format, arg1, arg2);
    }

    /**
     * This method is similar to {@link #trace(String, Object...)}
     * method except that the marker data is also taken into
     * consideration.
     *
     * @param marker   the marker data specific to this log statement
     * @param format   the format string
     * @param argArray an array of arguments
     * @since 1.4
     */
    public void trace(Marker marker, String format, Object... argArray) {
        logger.trace(marker, format, argArray);
    }

    /**
     * This method is similar to {@link #trace(String, Throwable)} method except that the
     * marker data is also taken into consideration.
     *
     * @param marker the marker data specific to this log statement
     * @param msg    the message accompanying the exception
     * @param t      the exception (throwable) to log
     * @since 1.4
     */
    public void trace(Marker marker, String msg, Throwable t) {
        logger.trace(marker, msg, t);
    }

    /**
     * Is the logger instance enabled for the DEBUG level?
     *
     * @return True if this Logger is enabled for the DEBUG level,
     *         false otherwise.
     */
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    /**
     * Log a message at the DEBUG level.
     *
     * @param msg the message string to be logged
     */
    public void debug(String msg) {
        logger.debug(msg);
    }

    /**
     * Log a message at the DEBUG level according to the specified format
     * and argument.
     * <p/>
     * <p>This form avoids superfluous object creation when the logger
     * is disabled for the DEBUG level. </p>
     *  @param format the format string
     * @param arg    the argument
     */
    public void debug(String format, Object arg) {
        logger.debug(format, arg);
    }

    /**
     * Log a message at the DEBUG level according to the specified format
     * and arguments.
     * <p/>
     * <p>This form avoids superfluous object creation when the logger
     * is disabled for the DEBUG level. </p>
     *  @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    public void debug(String format, Object arg1, Object arg2) {
        logger.debug(format, arg1, arg2);
    }

    /**
     * Log a message at the DEBUG level according to the specified format
     * and arguments.
     * <p/>
     * <p>This form avoids superfluous string concatenation when the logger
     * is disabled for the DEBUG level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
     * even if this logger is disabled for DEBUG. The variants taking
     * {@link #debug(String, Object) one} and {@link #debug(String, Object, Object) two}
     * arguments exist solely in order to avoid this hidden cost.</p>
     *  @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    public void debug(String format, Object... arguments) {
        logger.debug(format, arguments);
    }

    /**
     * Log an exception (throwable) at the DEBUG level with an
     * accompanying message.
     *  @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    public void debug(String msg, Throwable t) {
        logger.debug(msg, t);
    }

    /**
     * Similar to {@link #isDebugEnabled()} method except that the
     * marker data is also taken into account.
     *
     * @param marker The marker data to take into consideration
     * @return True if this Logger is enabled for the DEBUG level,
     *         false otherwise.
     */
    public boolean isDebugEnabled(Marker marker) {
        return logger.isDebugEnabled(marker);
    }

    /**
     * Log a message with the specific Marker at the DEBUG level.
     *  @param marker the marker data specific to this log statement
     * @param msg    the message string to be logged
     */
    public void debug(Marker marker, String msg) {
        logger.debug(marker, msg);
    }

    /**
     * This method is similar to {@link #debug(String, Object)} method except that the
     * marker data is also taken into consideration.
     *  @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg    the argument
     */
    public void debug(Marker marker, String format, Object arg) {
        logger.debug(marker, format, arg);
    }

    /**
     * This method is similar to {@link #debug(String, Object, Object)}
     * method except that the marker data is also taken into
     * consideration.
     *  @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        logger.debug(marker, format, arg1, arg2);
    }

    /**
     * This method is similar to {@link #debug(String, Object...)}
     * method except that the marker data is also taken into
     * consideration.
     *  @param marker    the marker data specific to this log statement
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    public void debug(Marker marker, String format, Object... arguments) {
        logger.debug(marker, format, arguments);
    }

    /**
     * This method is similar to {@link #debug(String, Throwable)} method except that the
     * marker data is also taken into consideration.
     *  @param marker the marker data specific to this log statement
     * @param msg    the message accompanying the exception
     * @param t      the exception (throwable) to log
     */
    public void debug(Marker marker, String msg, Throwable t) {
        logger.debug(marker, msg, t);
    }

    /**
     * Is the logger instance enabled for the INFO level?
     *
     * @return True if this Logger is enabled for the INFO level,
     *         false otherwise.
     */
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    /**
     * Log a message at the INFO level.
     *
     * @param msg the message string to be logged
     */
    public void info(String msg) {
        logger.info(msg);
    }

    /**
     * Log a message at the INFO level according to the specified format
     * and argument.
     * <p/>
     * <p>This form avoids superfluous object creation when the logger
     * is disabled for the INFO level. </p>
     *  @param format the format string
     * @param arg    the argument
     */
    public void info(String format, Object arg) {
        logger.info(format, arg);
    }

    /**
     * Log a message at the INFO level according to the specified format
     * and arguments.
     * <p/>
     * <p>This form avoids superfluous object creation when the logger
     * is disabled for the INFO level. </p>
     *  @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    public void info(String format, Object arg1, Object arg2) {
        logger.info(format, arg1, arg2);
    }

    /**
     * Log a message at the INFO level according to the specified format
     * and arguments.
     * <p/>
     * <p>This form avoids superfluous string concatenation when the logger
     * is disabled for the INFO level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
     * even if this logger is disabled for INFO. The variants taking
     * {@link #info(String, Object) one} and {@link #info(String, Object, Object) two}
     * arguments exist solely in order to avoid this hidden cost.</p>
     *  @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    public void info(String format, Object... arguments) {
        logger.info(format, arguments);
    }

    /**
     * Log an exception (throwable) at the INFO level with an
     * accompanying message.
     *  @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    public void info(String msg, Throwable t) {
        logger.info(msg, t);
    }

    /**
     * Similar to {@link #isInfoEnabled()} method except that the marker
     * data is also taken into consideration.
     *
     * @param marker The marker data to take into consideration
     * @return true if this logger is warn enabled, false otherwise
     */
    public boolean isInfoEnabled(Marker marker) {
        return logger.isInfoEnabled(marker);
    }

    /**
     * Log a message with the specific Marker at the INFO level.
     *  @param marker The marker specific to this log statement
     * @param msg    the message string to be logged
     */
    public void info(Marker marker, String msg) {
        logger.info(marker, msg);
    }

    /**
     * This method is similar to {@link #info(String, Object)} method except that the
     * marker data is also taken into consideration.
     *  @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg    the argument
     */
    public void info(Marker marker, String format, Object arg) {
        logger.info(marker, format, arg);
    }

    /**
     * This method is similar to {@link #info(String, Object, Object)}
     * method except that the marker data is also taken into
     * consideration.
     *  @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        logger.info(marker, format, arg1, arg2);
    }

    /**
     * This method is similar to {@link #info(String, Object...)}
     * method except that the marker data is also taken into
     * consideration.
     *  @param marker    the marker data specific to this log statement
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    public void info(Marker marker, String format, Object... arguments) {
        logger.info(marker, format, arguments);
    }

    /**
     * This method is similar to {@link #info(String, Throwable)} method
     * except that the marker data is also taken into consideration.
     *  @param marker the marker data for this log statement
     * @param msg    the message accompanying the exception
     * @param t      the exception (throwable) to log
     */
    public void info(Marker marker, String msg, Throwable t) {
        logger.info(marker, msg, t);
    }

    /**
     * Is the logger instance enabled for the WARN level?
     *
     * @return True if this Logger is enabled for the WARN level,
     *         false otherwise.
     */
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    /**
     * Log a message at the WARN level.
     *
     * @param msg the message string to be logged
     */
    public void warn(String msg) {
        logger.warn(msg);
    }

    /**
     * Log a message at the WARN level according to the specified format
     * and argument.
     * <p/>
     * <p>This form avoids superfluous object creation when the logger
     * is disabled for the WARN level. </p>
     *  @param format the format string
     * @param arg    the argument
     */
    public void warn(String format, Object arg) {
        logger.warn(format, arg);
    }

    /**
     * Log a message at the WARN level according to the specified format
     * and arguments.
     * <p/>
     * <p>This form avoids superfluous string concatenation when the logger
     * is disabled for the WARN level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
     * even if this logger is disabled for WARN. The variants taking
     * {@link #warn(String, Object) one} and {@link #warn(String, Object, Object) two}
     * arguments exist solely in order to avoid this hidden cost.</p>
     *  @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    public void warn(String format, Object... arguments) {
        logger.warn(format, arguments);
    }

    /**
     * Log a message at the WARN level according to the specified format
     * and arguments.
     * <p/>
     * <p>This form avoids superfluous object creation when the logger
     * is disabled for the WARN level. </p>
     *  @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    public void warn(String format, Object arg1, Object arg2) {
        logger.warn(format, arg1, arg2);
    }

    /**
     * Log an exception (throwable) at the WARN level with an
     * accompanying message.
     *  @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    public void warn(String msg, Throwable t) {
        logger.warn(msg, t);
    }

    /**
     * Similar to {@link #isWarnEnabled()} method except that the marker
     * data is also taken into consideration.
     *
     * @param marker The marker data to take into consideration
     * @return True if this Logger is enabled for the WARN level,
     *         false otherwise.
     */
    public boolean isWarnEnabled(Marker marker) {
        return logger.isWarnEnabled(marker);
    }

    /**
     * Log a message with the specific Marker at the WARN level.
     *  @param marker The marker specific to this log statement
     * @param msg    the message string to be logged
     */
    public void warn(Marker marker, String msg) {
        logger.warn(marker, msg);
    }

    /**
     * This method is similar to {@link #warn(String, Object)} method except that the
     * marker data is also taken into consideration.
     *  @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg    the argument
     */
    public void warn(Marker marker, String format, Object arg) {
        logger.warn(marker, format, arg);
    }

    /**
     * This method is similar to {@link #warn(String, Object, Object)}
     * method except that the marker data is also taken into
     * consideration.
     *  @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        logger.warn(marker, format, arg1, arg2);
    }

    /**
     * This method is similar to {@link #warn(String, Object...)}
     * method except that the marker data is also taken into
     * consideration.
     *  @param marker    the marker data specific to this log statement
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    public void warn(Marker marker, String format, Object... arguments) {
        logger.warn(marker, format, arguments);
    }

    /**
     * This method is similar to {@link #warn(String, Throwable)} method
     * except that the marker data is also taken into consideration.
     *  @param marker the marker data for this log statement
     * @param msg    the message accompanying the exception
     * @param t      the exception (throwable) to log
     */
    public void warn(Marker marker, String msg, Throwable t) {
        logger.warn(marker, msg, t);
    }

    /**
     * Is the logger instance enabled for the ERROR level?
     *
     * @return True if this Logger is enabled for the ERROR level,
     *         false otherwise.
     */
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    /**
     * Log a message at the ERROR level.
     *
     * @param msg the message string to be logged
     */
    public void error(String msg) {
        logger.error(msg);
    }

    /**
     * Log a message at the ERROR level according to the specified format
     * and argument.
     * <p/>
     * <p>This form avoids superfluous object creation when the logger
     * is disabled for the ERROR level. </p>
     *  @param format the format string
     * @param arg    the argument
     */
    public void error(String format, Object arg) {
        logger.error(format, arg);
    }

    /**
     * Log a message at the ERROR level according to the specified format
     * and arguments.
     * <p/>
     * <p>This form avoids superfluous object creation when the logger
     * is disabled for the ERROR level. </p>
     *  @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    public void error(String format, Object arg1, Object arg2) {
        logger.error(format, arg1, arg2);
    }

    /**
     * Log a message at the ERROR level according to the specified format
     * and arguments.
     * <p/>
     * <p>This form avoids superfluous string concatenation when the logger
     * is disabled for the ERROR level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
     * even if this logger is disabled for ERROR. The variants taking
     * {@link #error(String, Object) one} and {@link #error(String, Object, Object) two}
     * arguments exist solely in order to avoid this hidden cost.</p>
     *  @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    public void error(String format, Object... arguments) {
        logger.error(format, arguments);
    }

    /**
     * Log an exception (throwable) at the ERROR level with an
     * accompanying message.
     *  @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    public void error(String msg, Throwable t) {
        logger.error(msg, t);
    }

    /**
     * Similar to {@link #isErrorEnabled()} method except that the
     * marker data is also taken into consideration.
     *
     * @param marker The marker data to take into consideration
     * @return True if this Logger is enabled for the ERROR level,
     *         false otherwise.
     */
    public boolean isErrorEnabled(Marker marker) {
        return logger.isErrorEnabled(marker);
    }

    /**
     * Log a message with the specific Marker at the ERROR level.
     *  @param marker The marker specific to this log statement
     * @param msg    the message string to be logged
     */
    public void error(Marker marker, String msg) {
        logger.error(marker, msg);
    }

    /**
     * This method is similar to {@link #error(String, Object)} method except that the
     * marker data is also taken into consideration.
     *  @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg    the argument
     */
    public void error(Marker marker, String format, Object arg) {
        logger.error(marker, format, arg);
    }

    /**
     * This method is similar to {@link #error(String, Object, Object)}
     * method except that the marker data is also taken into
     * consideration.
     *  @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        logger.error(marker, format, arg1, arg2);
    }

    /**
     * This method is similar to {@link #error(String, Object...)}
     * method except that the marker data is also taken into
     * consideration.
     *  @param marker    the marker data specific to this log statement
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    public void error(Marker marker, String format, Object... arguments) {
        logger.error(marker, format, arguments);
    }

    /**
     * This method is similar to {@link #error(String, Throwable)}
     * method except that the marker data is also taken into
     * consideration.
     *  @param marker the marker data specific to this log statement
     * @param msg    the message accompanying the exception
     * @param t      the exception (throwable) to log
     */
    public void error(Marker marker, String msg, Throwable t) {
        logger.error(marker, msg, t);
    }
}
