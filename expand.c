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
PG_FUNCTION_INFO_V1(expand_partitioned_table_redistribute_leaf);

Datum expand_partitioned_table_prepare(PG_FUNCTION_ARGS)
{
    Oid root_oid = PG_GETARG_OID(0);
    LockRelationOid(root_oid, AccessExclusiveLock);
    GpPolicy *root_dist = GpPolicyFetch(root_oid);

    Assert(GpPolicyIsPartitioned(root_dist));//either random or hash

    List *children = find_all_inheritors(root_oid, AccessExclusiveLock, NULL);
    children = list_delete_first(children);

    int new_numsegments = getgpsegmentCount();

    /* xxx */
    root_dist->numsegments = new_numsegments;
    GpPolicyReplace(root_oid, root_dist);

    ListCell *child = NULL;
    GpPolicy *random_dist = createRandomPartitionedPolicy(new_numsegments);
    foreach (child, children)
    {
        Oid child_oid = lfirst_oid(child);
        char relkind = get_rel_relkind(child_oid);
        bool is_leaf = (relkind != RELKIND_PARTITIONED_TABLE &&
						relkind != RELKIND_PARTITIONED_INDEX);
        if (is_leaf)
            GpPolicyReplace(child_oid, random_dist);
        else
            GpPolicyReplace(child_oid, root_dist);
        // UnlockRelationOid(child_oid, AccessExclusiveLock);
    }
    
    list_free(children);
    pfree(root_dist);
    // UnlockRelationOid(root_oid, AccessExclusiveLock);
    PG_RETURN_DATUM(Int32GetDatum(0));
}

Datum expand_partitioned_table_redistribute_leaf(PG_FUNCTION_ARGS)
{
    Oid root_oid = PG_GETARG_OID(0);
    Oid leaf_oid = PG_GETARG_OID(1);
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
        LockRelationOid(root_oid, AccessShareLock);
        GpPolicy *root_dist = GpPolicyFetch(root_oid);
        Datum leaf_name = DirectFunctionCall1(regclassout, ObjectIdGetDatum(leaf_oid));
        if (GpPolicyIsHashPartitioned(root_dist))
        {
            Datum dist_by = DirectFunctionCall1(pg_get_table_distributedby, ObjectIdGetDatum(root_oid));
            appendStringInfo(
                &alter_table_cmd, "ALTER TABLE %s SET %s;", DatumGetCString(leaf_name), text_to_cstring(DatumGetTextP(dist_by)));
        }
        else
        {
            appendStringInfo(&alter_table_cmd, "ALTER TABLE %s EXPAND TABLE;", DatumGetCString(leaf_name));
        }
        ret = SPI_execute(alter_table_cmd.data, false, 0);
        if (ret != SPI_OK_UTILITY)
            elog(ERROR, "Redistribute partition %s failed.", DatumGetCString(leaf_name));
        pfree(root_dist);
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
