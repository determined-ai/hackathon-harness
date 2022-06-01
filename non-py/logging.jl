# LOG_FORMAT = "%(levelname)s: [%(process)s] %(name)s: %(message)s"
# levelname: Text logging level for the message ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
# process: Process ID (if available).
# name: Name of the logger used to log the call. (root)


#
# Python:
#
# logging.basicConfig(level=logging.DEBUG, format=det.LOG_FORMAT)
# # Log at different levels to demonstrate filter-by-level in the WebUI.
# logging.debug("debug-level message")
# logging.info("info-level message")
# logging.warning("warning-level message")
# logging.error("error-level message")

#
# Julia
#
function log_metafmt(level, _module, group, id, file, line)
    "$level: [$id] $group: $line\n"
end


using Logging
logger = ConsoleLogger(stderr, Logging.Debug, meta_formatter=log_metafmt)
global_logger(logger)

@debug "debug-level message"
@info "info-level message"
@warning "warning-level message"
@error "error-level message"
