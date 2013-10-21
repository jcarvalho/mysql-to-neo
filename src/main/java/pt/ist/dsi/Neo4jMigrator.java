package pt.ist.dsi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ist.fenixframework.dml.DomainClass;
import pt.ist.fenixframework.dml.DomainModel;
import pt.ist.fenixframework.dml.DomainRelation;
import pt.ist.fenixframework.dml.Role;
import pt.ist.fenixframework.dml.Slot;

public class Neo4jMigrator {

    private static final Logger logger = LoggerFactory.getLogger(Neo4jMigrator.class);

    private final DomainModel domainModel;
    private final Connection connection;
    private final BatchInserter inserter;
    private final BatchInserterIndexProvider index;
    private final long rootNode;
    private final Map<String, Integer> classIds = new HashMap<>();
    private final BatchInserterIndex allIndex;

    private final Map<String, String> indexConfig = new HashMap<>();

    public Neo4jMigrator(DomainModel domainModel, Connection connection, BatchInserter inserter) {
        super();
        this.domainModel = domainModel;
        this.connection = connection;
        this.inserter = inserter;
        index = new LuceneBatchInserterIndexProvider(inserter);
        indexConfig.put("type", "exact");
        allIndex = index.nodeIndex("ff$OID", indexConfig);
        this.rootNode = 0;
    }

    public void migrate() throws SQLException {
        inserter.createDeferredConstraint(DynamicLabel.label("FF_DOMAIN_CLASS")).on("domainClass").unique().create();

        loadDCI();

        loadObjects();
        loadRelations();

        inserter.createRelationship(rootNode, allIndex.get("oid", ValueContext.numeric(1l)).getSingle(),
                DynamicRelationshipType.withName("DOMAIN_ROOT"), null);

        index.shutdown();
        inserter.shutdown();
    }

    private void loadDCI() throws SQLException {
        for (DomainClass domClass : domainModel.getDomainClasses()) {
            Statement stmt = connection.createStatement();
            ResultSet rs =
                    stmt.executeQuery("SELECT * FROM FF$DOMAIN_CLASS_INFO where DOMAIN_CLASS_NAME = '" + domClass.getFullName()
                            + "'");

            rs.next();
            classIds.put(domClass.getFullName(), rs.getInt("DOMAIN_CLASS_ID"));
            rs.close();
            stmt.close();
        }

        for (Entry<String, Integer> entry : classIds.entrySet()) {
            Map<String, Object> nodeProps = new HashMap<>();
            nodeProps.put("domainClass", entry.getKey());
            nodeProps.put("classId", entry.getValue());
            long node = inserter.createNode(nodeProps, DynamicLabel.label("FF_DOMAIN_CLASS"));
            inserter.createRelationship(rootNode, node, DynamicRelationshipType.withName("DOMAIN_CLASS"), null);
        }
    }

    private void loadObjects() throws SQLException {
        for (DomainClass domClass : domainModel.getDomainClasses()) {

            BatchInserterIndex idx = index.nodeIndex(domClass.getFullName(), indexConfig);

            logger.info("Importing " + domClass.getFullName());

            int classId = classIds.get(domClass.getFullName());
            String tableName = getExpectedTableName(domClass);
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM `" + tableName + "` WHERE OID >> 32 = " + classId);

            long count = 0;

            while (rs.next()) {
                count++;
                Map<String, Object> properties = new HashMap<>();

                DomainClass cls = domClass;

                while (cls != null) {

                    for (Slot slot : cls.getSlotsList()) {
                        Object obj = rs.getObject(getDBName(slot.getName()));

                        if (obj instanceof Timestamp) {
                            Timestamp timestamp = (Timestamp) obj;

                            obj = new DateTime(timestamp.getTime()).getMillis();
                        }

                        if (obj != null) {
                            properties.put(slot.getName(), obj);
                        }
                    }

                    cls = (DomainClass) cls.getSuperclass();
                }

                Long oid = rs.getLong("OID");

                properties.put("oid", oid);

                long newNode = inserter.createNode(properties, DynamicLabel.label(domClass.getFullName().replace('.', '_')));

                Map<String, Object> indexProps = MapUtil.map("oid", ValueContext.numeric(oid));
                idx.add(newNode, indexProps);

                allIndex.add(newNode, indexProps);
            }

            rs.close();
            stmt.close();

            logger.info("\t\tCopied " + count + " objects.");
        }
    }

    private void loadRelations() throws SQLException {
        for (DomainRelation relation : domainModel.getDomainRelations()) {
            logger.info("Importing relation " + relation.getName());

            RelationshipType type = DynamicRelationshipType.withName(relation.getName());

            if (isManyToMany(relation)) {
                String tableName = getTableName(relation.getName());

                String roleOne = getFkName(relation.getFirstRole().getType().getName());
                String roleOther = getFkName(relation.getSecondRole().getType().getName());

                if (roleOne.equals(roleOther)) {
                    roleOne = roleOne + "_" + convertToDBStyle(relation.getFirstRole().getName());
                    roleOther = roleOther + "_" + convertToDBStyle(relation.getSecondRole().getName());
                }

                Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                int count = 0;
                while (rs.next()) {
                    Long firstOid = rs.getLong(roleOne);
                    Long otherOid = rs.getLong(roleOther);

                    Long first = allIndex.get("oid", ValueContext.numeric(firstOid)).getSingle();
                    Long other = allIndex.get("oid", ValueContext.numeric(otherOid)).getSingle();

                    inserter.createRelationship(first, other, type, null);
                    count++;
                }
                rs.close();
                stmt.close();

                logger.info("\tRelation is Many-to-Many, in table: {}, {} - {}. Got {} hits", tableName, roleOne, roleOther,
                        count);
            } else {

                Role picked;

                if (isOneToOne(relation)) {
                    picked = relation.getFirstRole().getName() == null ? relation.getFirstRole() : relation.getSecondRole();
                } else {
                    picked =
                            relation.getFirstRole().getMultiplicityUpper() != 1 ? relation.getFirstRole() : relation
                                    .getSecondRole();
                }

                String tableName = getExpectedTableName((DomainClass) picked.getType());

                String col = getFkName(picked.getOtherRole().getName());

                logger.info("\tLooking it up on table {}, column {}.", tableName, col);

                Statement stmt = connection.createStatement();
                ResultSet rs =
                        stmt.executeQuery("SELECT OID, " + col + " FROM `" + tableName + "` WHERE " + col + " is not null");

                int count = 0;

                while (rs.next()) {
                    Long firstOid = rs.getLong("OID");
                    Long otherOid = rs.getLong(col);

                    Long first = allIndex.get("oid", ValueContext.numeric(firstOid)).getSingle();
                    Long other = allIndex.get("oid", ValueContext.numeric(otherOid)).getSingle();

                    if (!picked.isFirstRole()) {
                        inserter.createRelationship(first, other, type, null);
                    } else {
                        inserter.createRelationship(other, first, type, null);
                    }

                    count++;
                }

                rs.close();
                stmt.close();

                logger.info("\t\tFinished. Got {} hits", count);
            }
        }
    }

    private boolean isOneToOne(DomainRelation relation) {
        return relation.getFirstRole().getMultiplicityUpper() == 1 && relation.getSecondRole().getMultiplicityUpper() == 1;
    }

    private boolean isManyToMany(DomainRelation relation) {
        return relation.getFirstRole().getMultiplicityUpper() != 1 && relation.getSecondRole().getMultiplicityUpper() != 1;
    }

    public static String getExpectedTableName(final DomainClass domainClass) {
        if (domainClass.getSuperclass() == null) {
            return getTableName(domainClass.getName());
        }
        return domainClass.getSuperclass() instanceof DomainClass ? getExpectedTableName((DomainClass) domainClass
                .getSuperclass()) : null;
    }

    private static String getTableName(final String name) {
        final StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (final char c : name.toCharArray()) {
            if (isFirst) {
                isFirst = false;
                stringBuilder.append(Character.toUpperCase(c));
            } else {
                if (Character.isUpperCase(c)) {
                    stringBuilder.append('_');
                    stringBuilder.append(c);
                } else {
                    stringBuilder.append(Character.toUpperCase(c));
                }
            }
        }
        return stringBuilder.toString();
    }

    public static String getFkName(String slotName) {
        return "OID_" + convertToDBStyle(slotName);
    }

    public static String convertToDBStyle(String string) {
        StringBuilder result = new StringBuilder(string.length() + 10);
        boolean first = true;
        for (char c : string.toCharArray()) {
            if (first) {
                first = false;
            } else if (Character.isUpperCase(c)) {
                result.append('_');
            }
            result.append(Character.toUpperCase(c));
        }

        return result.toString();
    }

    public static String getDBName(String name) {

        StringBuilder str = new StringBuilder();

        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);

            if (Character.isUpperCase(c) && i != 0) {
                str.append("_");
            }

            str.append(Character.toUpperCase(c));
        }

        return str.toString();
    }

}
