

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

package stroom.stats.hbase.table.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import stroom.stats.hbase.structure.RowKeyTagValue;
import stroom.stats.hbase.structure.TagValueFilterTreeNode;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import stroom.stats.util.logging.LambdaLogger;

import java.io.IOException;
import java.util.Arrays;

/**
 * Abstract filter class for filtering hbase tables by the tag/value section of
 * their row key.
 *
 * Implementing classes must provide an implementation to
 * findTagValueSectionOfRowKey() as each implementation may have a different
 * rowkey structure
 *
 * The fitler relies on the tag/value pairs in the rowkey being in lexographical
 * order by tag uid so it can make optimisations when filtering the tags
 */
public abstract class AbstractTagValueFilter extends FilterBase {
    private static LambdaLogger LOGGER = LambdaLogger.getLogger(AbstractTagValueFilter.class);

    // instance variable to hold the tree of filter terms and logic
    private final TagValueFilterTree tagValueFilterTree;

    // instance variable to record the outcome of the filterRowKey method and to
    // prevent further filtering on the row data
    private boolean filterOutRow = false;

    public AbstractTagValueFilter(final TagValueFilterTree tagValueFilterTree) {
        this.tagValueFilterTree = tagValueFilterTree;

        LOGGER.trace(() -> String.format( "Dumping contents of tagValueFilterTree: "
                + (tagValueFilterTree == null ? "NULL" : tagValueFilterTree.toString()) ));

    }

    @Override
    public void reset() {
        this.filterOutRow = false;
    }

    /**
     * Overrides the filterRowKey method on the HBase FilterBase class. Tests
     * each row key against the logic defined in the {@link TagValueFilterTree}
     *
     * @param buffer
     *            the byte array containing the row key
     * @param offset
     *            the offset in the byte array where the row key begins
     * @param length
     *            the length of the row key in the byte array
     * @return True if we are filtering OUT the row, false if keeping it
     * @throws IOException
     */
    @Override
    public boolean filterRowKey(final byte[] buffer, final int offset, final int length) throws IOException {
        LOGGER.trace(() -> String.format("Evaluating rowkey: ["
                + ByteArrayUtils.byteArrayToHex(Arrays.copyOfRange(buffer, offset, (offset + length))) + "]"));

        boolean filterOutRow = false;

        // if we have been passed an empty filter tree then there is no point
        // deconstructing the row key or trying to
        // crawl the tree. Therefore the row is a match and not filtered out
        if (tagValueFilterTree != null && tagValueFilterTree.getRootNode() != null) {
            final int[] tagValuePairStartPositions = deconstructRowKeyTagValues(buffer, offset, length);

            final boolean hasMatched = tagValueFilterTreeNodeCrawler(tagValueFilterTree.getRootNode(), buffer,
                    tagValuePairStartPositions, "  ");

            // we think in terms of matches, this method thinks in terms of
            // filtering out so inverse the value
            filterOutRow = !hasMatched;
        }

        return filterOutRow;
    }

    abstract TagValueSection findTagValueSectionOfRowKey(final byte[] buffer, final int offset, final int length);

    /**
     * Extracts the start positions of each of the tag value pair sections in
     * the row key. This is done rather than extracting chunks from the buffer
     * to avoid expensive array copy operati.ons
     *
     * @param buffer
     *            the byte array containing the row key
     * @param offset
     *            the offset in the byte array where the row key begins
     * @param length
     *            the length of the row key in the byte array
     * @return an array of start positions of each tag value pair in the passed
     *         buffer
     */
    private int[] deconstructRowKeyTagValues(final byte[] buffer, final int offset, final int length) {
        int[] retVal;

        final TagValueSection tagValueSection = findTagValueSectionOfRowKey(buffer, offset, length);

        final int lengthOfPairsSection = tagValueSection.getLengthOfSection();

        if (lengthOfPairsSection == 0) {
            // no tag values so return an empty array
            retVal = new int[0];
        } else {
            int pos = tagValueSection.from;
            final int pairsCount = lengthOfPairsSection / (UID.UID_ARRAY_LENGTH * 2);

            // make sure we have a valid set of tagValue pairs to work with
            if (lengthOfPairsSection % RowKeyTagValue.TAG_AND_VALUE_ARRAY_LENGTH != 0)
                throw new RuntimeException(String.format(
                        "TagValue portion of rowkey is not a valid length.  TagValue portion length [%s], row key[%s]",
                        lengthOfPairsSection, ByteArrayUtils.byteArrayToHex(Arrays.copyOfRange(buffer, pos, length))));

            retVal = new int[pairsCount];

            // loop through the pairs in the row key to build an array of pairs
            for (int i = 0; i < pairsCount; i++) {
                retVal[i] = pos;
                pos += RowKeyTagValue.TAG_AND_VALUE_ARRAY_LENGTH;
            }
        }
        return retVal;
    }

    /**
     * Recursive method to crawl the filterTermsTree and evaluate each node
     * against the tag value pairs
     *
     * @param node
     *            a Node that could be one of TermNode or OperationNode
     * @param buffer
     *            the byte array containing the row key
     * @param tagValuePairStartPositions
     *            An array of the tagValue pairs (as byte arrays)
     * @return true if the evaluation of the term node is true or the overall
     *         evaluation of all children in an operationNode is true. E.g. if
     *         the opMode is AND then all child nodes must evaluate to true
     */
    private boolean tagValueFilterTreeNodeCrawler(final TagValueFilterTreeNode node, final byte[] buffer,
            final int[] tagValuePairStartPositions, final String logPrefix) {
        boolean hasMatched = false;

        LOGGER.trace(() -> String.format(logPrefix + "Filter node: " + (node == null ? "NULL" : node.toString())));

        // work out what kind of node we are dealing with and deal with it
        // accordingly
        if (node instanceof RowKeyTagValue) {
            // got a term so evaluate it
            hasMatched = evaluateTagValueNode((RowKeyTagValue) node, buffer, tagValuePairStartPositions,
                    logPrefix + "  ");

        } else if (node instanceof TagValueOperatorNode) {
            // got an operation node so do the following depending on the
            // operation
            // AND - evaluate all children and return true if all are true
            // OR - evaluate each child and return true as soon as one is true
            // NOT - evaluate the one child and return the inverse of its return
            // value
            final TagValueOperatorNode opNode = (TagValueOperatorNode) node;

            if (opNode.getFilterOperationMode().equals(FilterOperationMode.NOT)) {
                // NOT - so inverse the one child
                hasMatched = !tagValueFilterTreeNodeCrawler(opNode.getChildren().get(0), buffer,
                        tagValuePairStartPositions, logPrefix + "  ");

                LOGGER.trace(String.format(logPrefix + "NOT operation, reversing value from the child node"));

            } else {
                // AND or OR - evaluate each child
                for (final TagValueFilterTreeNode childNode : ((TagValueOperatorNode) node).getChildren()) {
                    hasMatched = tagValueFilterTreeNodeCrawler(childNode, buffer, tagValuePairStartPositions,
                            logPrefix + "  ");

                    if (opNode.getFilterOperationMode().equals(FilterOperationMode.AND) && hasMatched == false) {
                        // AND - so if we haven't found it yet then drop out as
                        // we need all to match
                        break;
                    } else if (opNode.getFilterOperationMode().equals(FilterOperationMode.OR) && hasMatched == true) {
                        // OR - found a match so drop out as there is no need to
                        // test the rest
                        break;
                    }
                }
            }
        } else {
            throw new RuntimeException(
                    "Passed a subclass of Node that we weren't expecting: " + node.getClass().getName());
        }

        //Can't use lamda here as hasMatched is not final
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(logPrefix + "Filter node result: " + (hasMatched ? "TRUE" : "FALSE"));
        }

        return hasMatched;
    }

    private boolean evaluateTagValueNode(final RowKeyTagValue tagValueNode, final byte[] buffer,
            final int[] tagValuePairStartPositions, final String logPrefix) {
        boolean hasMatched = false;

        // if the value in the filter node is null then it means we do not have
        // a UID for the filter value so it
        // could never have been put into the event store in the first place,
        // thus it can never be a match so there is
        // no point in testing the rowkey
        if (tagValueNode.getValue() != null) {
            // loop through each tag value pair start position and test against
            // the tagValue pair in the filter node
            for (final int startPosition : tagValuePairStartPositions) {
                LOGGER.trace(() -> String.format(logPrefix + "Eval pair at position [%s]", startPosition));

                // compare the tagValue from the TermNode (in UID form) against
                // the
                // one from the row key

                // performance optimisation to break out of the loop if we see
                // that
                // the UID of the filter node is less than the UID of the pair
                // we
                // are currently looking at in the loop. The UIDs in the row key
                // are
                // built in lexicographical order. e.g. UID from the filter node
                // is
                // [0,0,0,1] and the UID from the row key we are currently on is
                // [0,0,0,2], so there is no point searching any further as we
                // will
                // never find it.
                if (tagValueNode.getTag().compareTo(buffer, startPosition) < 0) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("  " + logPrefix + "Breaking out of loop as gone past UID of interest");
                    }
                    break;
                }

                // now compare the full pair from the filter node against the
                // current pair from the row key
                final int compareResult = Bytes.compareTo(buffer, startPosition,
                        RowKeyTagValue.TAG_AND_VALUE_ARRAY_LENGTH, tagValueNode.asByteArray(), 0,
                        tagValueNode.asByteArray().length);

                if (compareResult == 0) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("  " + logPrefix + "Breaking out of loop as match found");
                    }
                    hasMatched = true;
                    break;
                }
            }
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(logPrefix + "Eval pair from rowkey result: " + (hasMatched ? "TRUE" : "FALSE"));
        }
        return hasMatched;
    }

    @Override
    public ReturnCode filterKeyValue(final Cell v) {
        // if we have already determined that this row is not a match then jump
        // to the next row otherwise keep the row
        if (this.filterOutRow) {
            return ReturnCode.NEXT_ROW;
        }
        return ReturnCode.INCLUDE;
    }

    /**
     * Convert the filterTermsTree instance variable to a byte[] so the filter state
     * can be passed to the HBase server
     *
     * @see org.apache.hadoop.hbase.filter.FilterBase#toByteArray()
     */
    @SuppressWarnings("unused")
    @Override
    public byte[] toByteArray() {
        return TagValueFilterTreeSerialiser.instance().serialize(tagValueFilterTree);
    }

    @Override
    public String toString() {
        return tagValueFilterTree.toString();
    }

    static class TagValueSection {
        private final int from;
        private final int to;

        public TagValueSection(final int from, final int to) {
            this.from = from;
            this.to = to;
        }

        public int getFrom() {
            return from;
        }

        public int getTo() {
            return to;
        }

        public int getLengthOfSection() {
            return to - from;
        }

        @Override
        public String toString() {
            return "TagValueSection [from=" + from + ", to=" + to + "]";
        }
    }
}
