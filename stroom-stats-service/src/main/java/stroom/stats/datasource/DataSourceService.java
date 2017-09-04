package stroom.stats.datasource;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.ExpressionTerm;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationService;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DataSourceService {

    private static final FieldDefinition FIELD_DEF_DATE_TIME = FieldDefinition.of(
            DataSourceField.DataSourceFieldType.DATE_FIELD,
            true,
            ExpressionTerm.Condition.BETWEEN,
            ExpressionTerm.Condition.EQUALS,
            ExpressionTerm.Condition.GREATER_THAN,
            ExpressionTerm.Condition.GREATER_THAN_OR_EQUAL_TO,
            ExpressionTerm.Condition.LESS_THAN,
            ExpressionTerm.Condition.LESS_THAN_OR_EQUAL_TO);

    private static final FieldDefinition FIELD_DEF_NON_QUERYABLE_FIELD = FieldDefinition.of(
            DataSourceField.DataSourceFieldType.FIELD,
            false);

    private static final FieldDefinition FIELD_DEF_NON_QUERYABLE_NUMERIC = FieldDefinition.of(
            DataSourceField.DataSourceFieldType.NUMERIC_FIELD,
            false);

    private static final FieldDefinition FIELD_DEF_PRECISION = FieldDefinition.of(
            DataSourceField.DataSourceFieldType.FIELD,
            true,
            ExpressionTerm.Condition.EQUALS);

    private static final FieldDefinition FIELD_DEF_TAGS = FieldDefinition.of(
            DataSourceField.DataSourceFieldType.FIELD,
            true,
            ExpressionTerm.Condition.EQUALS,
            ExpressionTerm.Condition.IN);

    private static final ImmutableMap<String, FieldDefinition> FIELD_DEFINITION_MAP =
            ImmutableMap.<String, FieldDefinition>builder()
                    .put(StatisticConfiguration.FIELD_NAME_STATISTIC, FIELD_DEF_NON_QUERYABLE_FIELD)
                    .put(StatisticConfiguration.FIELD_NAME_DATE_TIME, FIELD_DEF_DATE_TIME)
                    .put(StatisticConfiguration.FIELD_NAME_COUNT, FIELD_DEF_NON_QUERYABLE_NUMERIC)
                    .put(StatisticConfiguration.FIELD_NAME_VALUE, FIELD_DEF_NON_QUERYABLE_NUMERIC)
                    .put(StatisticConfiguration.FIELD_NAME_MIN_VALUE, FIELD_DEF_NON_QUERYABLE_NUMERIC)
                    .put(StatisticConfiguration.FIELD_NAME_MAX_VALUE, FIELD_DEF_NON_QUERYABLE_NUMERIC)
                    .put(StatisticConfiguration.FIELD_NAME_PRECISION, FIELD_DEF_PRECISION)
                    .put(StatisticConfiguration.FIELD_NAME_PRECISION_MS, FIELD_DEF_NON_QUERYABLE_NUMERIC)
                    .build();

    private final StatisticConfigurationService statisticConfigurationService;

    @Inject
    public DataSourceService(final StatisticConfigurationService statisticConfigurationService) {
        this.statisticConfigurationService = statisticConfigurationService;
    }

    public Optional<DataSource> getDatasource(final DocRef docRef) {
        return statisticConfigurationService.fetchStatisticConfigurationByUuid(docRef.getUuid())
                .map(DataSourceService::statConfigToDataSourceMapper);
    }

    private static DataSource statConfigToDataSourceMapper(final StatisticConfiguration statisticConfiguration) {
        Preconditions.checkNotNull(statisticConfiguration);

        List<DataSourceField> fields = statisticConfiguration.getAllFieldNames().stream()
                .map(field -> fieldMapper(field, statisticConfiguration))
                .collect(Collectors.toList());

        return new DataSource(fields);
    }

    private static DataSourceField fieldMapper(final String fieldName,
                                               final StatisticConfiguration statisticConfiguration) {
        Preconditions.checkNotNull(fieldName);
        FieldDefinition fieldDefinition = FIELD_DEFINITION_MAP.get(fieldName);
        if (fieldDefinition == null && statisticConfiguration.isDynamicField(fieldName)) {
            fieldDefinition = FIELD_DEF_TAGS;
        }
        Preconditions.checkNotNull(fieldDefinition,"No field definition found for field %s", fieldName);
        return fieldDefinition.toDataSourceField(fieldName);
    }


    private static class FieldDefinition {

        private final DataSourceField.DataSourceFieldType fieldType;
        private final boolean isQueryable;
        private final List<ExpressionTerm.Condition> supportedConditions;

        private FieldDefinition(final DataSourceField.DataSourceFieldType fieldType,
                                final boolean isQueryable,
                                final List<ExpressionTerm.Condition> supportedConditions) {
            this.fieldType = fieldType;
            this.isQueryable = isQueryable;
            this.supportedConditions = supportedConditions;
        }

        public static FieldDefinition of(final DataSourceField.DataSourceFieldType fieldType,
                                         final boolean isQueryable,
                                         final ExpressionTerm.Condition... supportedConditions) {
            return new FieldDefinition(fieldType, isQueryable, Arrays.asList(supportedConditions));
        }

        public DataSourceField.DataSourceFieldType getFieldType() {
            return fieldType;
        }

        public boolean isQueryable() {
            return isQueryable;
        }

        public List<ExpressionTerm.Condition> getSupportedConditions() {
            return supportedConditions;
        }

        public DataSourceField toDataSourceField(final String fieldName) {
            Preconditions.checkNotNull(fieldName);
            return new DataSourceField(fieldType, fieldName, isQueryable, supportedConditions);
        }
    }
}
