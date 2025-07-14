using aws_backup_common;

namespace aws_backup_commands;

public class ContextResolver(Configuration configuration)
    : ContextResolverBase(configuration, configuration.ClientId), IContextResolver
{
    public override string PathsToArchive()
    {
        return "";
    }

    public override string CronSchedule()
    {
        return "";
    }
}