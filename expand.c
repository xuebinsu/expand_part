#include "postgres.h"
#include "fmgr.h"

#include "utils/rel.h"
#include "access/relation.h"
#include "catalog/gp_distribution_policy.h"
#include "catalog/pg_inherits.h"
#include "cdb/cdbutil.h"
#include "storage/lmgr.h"
#include "executor/spi.h"
#include "utils/syscache.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(expand_partitioned_table_prepare);
PG_FUNCTION_INFO_V1(expand_partitioned_table_redistribute_part);

Datum expand_partitioned_table_prepare(PG_FUNCTION_ARGS)
{
    Oid root_oid = PG_GETARG_OID(0);

    Relation root_rel = relation_open(root_oid, AccessExclusiveLock);

    List *children = find_all_inheritors(root_oid, AccessExclusiveLock, NULL);
    children = list_delete_first(children);

    int new_numsegments = getgpsegmentCount();

    ListCell *child = NULL;
    GpPolicy *random_dist = createRandomPartitionedPolicy(new_numsegments);
    foreach (child, children)
    {
        Oid child_oid = lfirst_oid(child);
        Relation child_rel = relation_open(child_oid, NoLock);
        if (child_rel->rd_cdbpolicy->numsegments == new_numsegments ||
            !GpPolicyIsHashPartitioned(child_rel->rd_cdbpolicy))
        {
            relation_close(child_rel, NoLock);
            UnlockRelationId(&(child_rel->rd_lockInfo.lockRelId), AccessExclusiveLock);
            continue;
        }
        GpPolicyReplace(child_oid, random_dist);
        relation_close(child_rel, NoLock);
        UnlockRelationId(&(child_rel->rd_lockInfo.lockRelId), AccessExclusiveLock);
    }
    GpPolicy *root_dist = GpPolicyFetch(root_oid);
    root_dist->numsegments = new_numsegments;
    GpPolicyReplace(root_oid, root_dist);
    relation_close(root_rel, AccessExclusiveLock);

    PG_RETURN_DATUM(Int32GetDatum(0));
}

Datum expand_partitioned_table_redistribute_part(PG_FUNCTION_ARGS)
{
    Oid root_oid = PG_GETARG_OID(0);
    Oid part_oid = PG_GETARG_OID(1);
    bool connected = false;
    int ret = 0;

    PG_TRY();
    {
        ret = SPI_connect();
        if (ret != SPI_OK_CONNECT)
            elog(ERROR, "Connection error, code=%d", ret);
        connected = true;

        StringInfoData alter_table_cmd;
        initStringInfo(&alter_table_cmd);

        GpPolicy *root_dist = GpPolicyFetch(root_oid);
        Datum part_name = DirectFunctionCall1(regclassout, ObjectIdGetDatum(part_oid));
        if (GpPolicyIsHashPartitioned(root_dist))
        {
            Datum dist_by = DirectFunctionCall1(pg_get_table_distributedby, ObjectIdGetDatum(root_oid));
            appendStringInfo(
                &alter_table_cmd, "ALTER TABLE %s SET %s;", DatumGetName(part_name)->data, text_to_cstring(DatumGetTextP(dist_by)));
        }
        else
        {
            appendStringInfo(&alter_table_cmd, "ALTER TABLE %s EXPAND TABLE;", DatumGetName(part_name)->data);
        }
        ret = SPI_execute(alter_table_cmd.data, false, 0);
        if (ret != SPI_OK_UTILITY)
            elog(ERROR, "Redistribute partition %s failed.", DatumGetName(part_name)->data);
    }
    PG_CATCH();
    {
        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();
        EmitErrorReport();
        FlushErrorState();
        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();
    }
    PG_END_TRY();

    if (connected)
    {
        SPI_finish();
    }

    PG_RETURN_DATUM(Int32GetDatum(ret));
}
