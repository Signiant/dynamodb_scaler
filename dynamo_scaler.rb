#!/usr/local/bin/ruby

%w[ getoptlong pp base64 openssl time yaml net/smtp ].each { |f| require f }

require 'rubygems'
require 'bundler/setup'

require 'aws-sdk-v1'
require 'json'

# Turn off buffering
STDOUT.sync = true

CODE_OK = 0
CODE_WARNING = 1
CODE_CRITICAL = 2
CODE_UNKNOWN = 3

DYNAMODB_STATUS_CREATING = "CREATING" 
DYNAMODB_STATUS_DELETING = "DELETING" 
DYNAMODB_STATUS_UPDATING = "UPDATING" 
DYNAMODB_STATUS_ACTIVE = "ACTIVE" 
DYNAMODB_STATUS_UNKNOWN = "UNKNOWN"
DYNAMODB_NAMESPACE = "AWS/DynamoDB"

CLOUDWATCH_PERIOD = 300

STATE_OK = "OK"
STATE_UNKNOWN = "UNKNOWN"
STATE_ERROR = "ERROR"

@service_output = "OK"
@service_perfdata = ""
@service_table_list = ""
@rc = CODE_OK

@log_dir =  '/var/log'

@dynamo_db_endpoint = 'dynamodb.us-east-1.amazonaws.com'
@frequency = 300

@dynamo_regions = {};
@dynamo_regions["US East (Northern Virginia)"] =  "dynamodb.us-east-1.amazonaws.com"
@dynamo_regions["US West (Northern California)"] = "dynamodb.us-west-1.amazonaws.com"
@dynamo_regions["US West (Oregon)"] =  "dynamodb.us-west-2.amazonaws.com"
@dynamo_regions["EU (Ireland)"] =  "dynamodb.eu-west-1.amazonaws.com"
@dynamo_regions["Asia Pacific (Tokyo)"] =  "dynamodb.ap-northeast-1.amazonaws.com"
@dynamo_regions["Asia Pacific (Singapore)"] =  "dynamodb.ap-southeast-1.amazonaws.com"
@dynamo_regions["Asia Pacific (Sydney)"] =  "dynamodb.ap-southeast-2.amazonaws.com"
@dynamo_regions["South America (Sao Paulo)"] =  "dynamodb.sa-east-1.amazonaws.com"

@cloudwatch_timer = 900

@threshold_over = 0.5
#@default_threshold_increase = 1.5


@test_mode = false

@start_time = Time.now()

@cwApi = nil
@dynamoApiClient = nil

@dynamodb_tables = {}
@dynamodb_tables_to_update = {}

@config_file = nil
@ignore_file = nil

@output_file = "table_description.csv"

@log_output = false
@log_output_id = "all"
@log_dir =  "#{@log_dir}/dynamo_scaler"
@logger = nil
@log_file_name = "dynamo_scaler.log"

@std_out_print = true

# specify the options we accept and initialize and the option parser
@verbose = false 
use_rsa = false
$my_pid = Process.pid

#####################################################
class Provision_capacity
	attr_accessor :currentReadUnits
	attr_accessor :currentWriteUnits
	attr_accessor :currentCloudWatchReadUnits
	attr_accessor :currentCloudWatchWriteUnits
	attr_accessor :originalReadUnits
	attr_accessor :originalWriteUnits
	attr_accessor :nextReadUnits
	attr_accessor :nextWriteUnits
	attr_accessor :desiredReadUnits
	attr_accessor :desiredWriteUnits
	attr_accessor :increaseReadUnitsByPercentage
	attr_accessor :increaseWriteUnitsByPercentage
	attr_accessor :increaseReadPercentage
	attr_accessor :increaseWritePercentage
	def initialize ( ) 
		@currentReadUnits = 5
		@currentWriteUnits = 5
		@currentCloudWatchReadUnits = 1
		@currentCloudWatchWriteUnits = 1
		@originalReadUnits = 1
		@originalWriteUnits = 1
		@nextReadUnits = 5
		@nextWriteUnits = 5
		@desiredReadUnits = nil
		@desiredWriteUnits = nil
		@increaseReadUnitsByPercentage = 50
		@increaseWriteUnitsByPercentage = 50
		@increaseReadPercentage = false
		@increaseWritePercentage = false
	end
end
#####################################################
class Global_index
	attr_accessor :name
	attr_accessor :index_status
	attr_accessor :capacity
	def initialize ( name ) 
		@name = name
		@index_status = nil
		@capacity = Provision_capacity.new
	end
end
#####################################################
class Dynamodb_table
	attr_accessor :name
	attr_accessor :status
	attr_accessor :aws_status
	attr_accessor :state
	attr_accessor :update_units
	attr_accessor :update_state
	attr_accessor :capacity
	attr_accessor :globalIndexes
	def initialize ( table_name) 
		@name = table_name
		@aws_status = nil
		@state = STATE_UNKNOWN
		@update_units = false
		@update_state = STATE_UNKNOWN
		@capacity = Provision_capacity.new
		@globalIndexes = []

	end
end

#####################################################
class Dynamodb_table
	attr_accessor :name
	attr_accessor :status
	attr_accessor :state
	attr_accessor :aws_status
	attr_accessor :update_units
	attr_accessor :update_state
	attr_accessor :throttledRequestsScan
	attr_accessor :throttledRequestsQuery
	attr_accessor :throttledRequestsUpdateItem
	attr_accessor :systemErrorsScan
	attr_accessor :systemErrorsQuery
	attr_accessor :systemErrorsUpdateItem
	attr_accessor :capacity
	attr_accessor :globalIndexes
	def initialize ( table_name) 
		@name = table_name
		@aws_status = nil
		@state = STATE_UNKNOWN
		@update_units = false
		@update_state = STATE_UNKNOWN
		@throttledRequestsScan = nil
		@throttledRequestsQuery = nil
		@throttledRequestsUpdateItem = nil
		@systemErrorsScan = nil
		@systemErrorsQuery = nil
		@systemErrorsUpdateItem = nil
		@capacity = Provision_capacity.new
		@globalIndexes = []
	end
end

#####################################################
def myLogger(line)
	if @log_output == true
		if @logger.nil?
			@logger = File.open("#{@log_dir}/#{@log_file_name}", 'a')
			@logger.sync = true
		end
		@logger.puts Time.now.strftime("%H:%M:%S") + " " + line
	end
end


#####################################################
def myPuts(line, forcePrint = false, writeFile = false)
	if forcePrint == true
		puts line
	else
		puts line if @verbose == true
	end
	myLogger(line) if writeFile == true
end


#####################################################
def set_capacity_from_config ( capacity, section)
	if section.include?("setReadCapacityUnitsTo")
		capacity.desiredReadUnits = section["setReadCapacityUnitsTo"]
	end
	if section.include?("setWriteCapacityUnitsTo")
		capacity.desiredWriteUnits = section["setWriteCapacityUnitsTo"]
	end
	if section.include?("increaseReadCapacityUnitsByPercentage")
		capacity.increaseReadUnitsByPercentage = section["increaseReadCapacityUnitsByPercentage"]
		capacity.increaseReadPercentage = true
	end
	if section.include?("increaseWriteCapacityUnitsByPercentage")
		capacity.increaseWriteUnitsByPercentage = section["increaseWriteCapacityUnitsByPercentage"]
		capacity.increaseWritePercentage = true
	end
end


#####################################################
def read_config(config_file)
        begin
                config = YAML.load_file(config_file)
        rescue Exception => e
            # myPuts "Caught Exception #{e} reading config"
            return CODE_UNKNOWN, "Caught Exception #{e} reading config"
        end
        if config.include?("dynamodb")
		config["dynamodb"].each_key do |table_name|
			@dynamodb_tables[table_name]  = Dynamodb_table.new(table_name)
			set_capacity_from_config( @dynamodb_tables[table_name].capacity, config["dynamodb"][table_name])
			if config["dynamodb"][table_name].include?("global_indexes")
			    config["dynamodb"][table_name]["global_indexes"].each_key do | index_name |
			        global_index = Global_index.new(index_name)
				set_capacity_from_config( global_index.capacity, config["dynamodb"][table_name]["global_indexes"][index_name])
				@dynamodb_tables[table_name].globalIndexes.push(global_index)

			    end
			        
			end
		end
        end
        if config.include?("debug")
                if config["debug"].include?("verbose")
                        if @verbose == false
                                if Integer(config["debug"]["verbose"]) == 1
					@verbose = true
				end
                        end
                end
        end
        return CODE_OK, "reading config complete"
end



#####################################################
def read_ignore(ignore_file)
        begin
                ignore = YAML.load_file(ignore_file)
        rescue Exception => e
            myPuts "Caught Exception #{e} reading ignore"
            return CODE_UNKNOWN, "Caught Exception #{e} reading ignore"
        end
        if ignore.include?("endtime")
	    myPuts "ignore endtime #{ignore['endtime']}"
	    if ignore["endtime"] >= Time.now
		if ignore.include?("dynamodb")
		    if ignore["dynamodb"] != nil
			ignore["dynamodb"].each do |table_name|
			    if @dynamodb_tables.include?(table_name) 
				myPuts "ignoring #{table_name}" 
				@dynamodb_tables.delete(table_name)
			    end
			end
		    end

		end
	    end
        end

        return CODE_OK, "reading ignore complete"
end

#####################################################
def compareCurrentToDesiredUnits( table)
    equal = true
    if  (table.capacity.currentWriteUnits == table.capacity.desiredWriteUnits) &&
	(table.capacity.currentReadUnits == table.capacity.desiredReadUnits) 
	    table.globalIndexes.each do | index |
		if index.capacity.currentReadUnits != index.capacity.desiredReadUnits 
		    equal = false
		end
		if index.capacity.currentWriteUnits != index.capacity.desiredWriteUnits 
		    equal = false
		end
	    end
    else
        equal = false
    end

    return equal
end


#####################################################
def compareCurrentGtEqDesiredUnits( table)
    equal = true
    if  (table.capacity.currentWriteUnits >= table.capacity.desiredWriteUnits) &&
	(table.capacity.currentReadUnits >= table.capacity.desiredReadUnits) 
	    table.globalIndexes.each do | index |
		if index.capacity.currentReadUnits < index.capacity.desiredReadUnits 
		    equal = false
		end
		if index.capacity.currentWriteUnits < index.capacity.desiredWriteUnits 
		    equal = false
		end
	    end
    else
        equal = false
    end

    return equal
end



####################################################
def getCurrentUnits(dynamoApiClient, table_name,table)

	state = STATE_OK
	aws_status = DYNAMODB_STATUS_UNKNOWN

	begin

		dyn_resp = dynamoApiClient.describe_table( options={:table_name=>table_name})

		pp dyn_resp if @verbose == true
		if dyn_resp.include?(:table)
			if dyn_resp[:table].include?(:table_status)
				aws_status =dyn_resp[:table][:table_status]
			else
				myPuts "Error: no Table Status, going to next"
				state = STATE_ERROR
			end
			if dyn_resp[:table].include?(:provisioned_throughput)
				if dyn_resp[:table][:provisioned_throughput].include?(:read_capacity_units) 
					table.capacity.currentReadUnits = dyn_resp[:table][:provisioned_throughput][:read_capacity_units] 
				else 
					myPuts "Error: no Table ReadCapacityUnits, going to next"
					state = STATE_ERROR
				end
				if dyn_resp[:table][:provisioned_throughput].include?(:write_capacity_units) 
					table.capacity.currentWriteUnits = dyn_resp[:table][:provisioned_throughput][:write_capacity_units] 
				else 
					myPuts "Error: no Table WriteCapacityUnits, going to next"
					state = STATE_ERROR
				end
				
			else
				myPuts "Error: no Table ProvisionedThroughout, going to next"
				state = STATE_ERROR
			end	
			if dyn_resp[:table].include?(:global_secondary_indexes)
			    dyn_resp[:table][:global_secondary_indexes].each do | index | 
			    	table.globalIndexes.each do | update_index |
				    myPuts " index name #{index[:index_name]} update index #{update_index.name}"
				    if index[:index_name] == update_index.name
				    	update_index.index_status = index[:index_status]
					if index[:provisioned_throughput].include?(:read_capacity_units) 
					    update_index.capacity.currentReadUnits = index[:provisioned_throughput][:read_capacity_units]
					else 
						myPuts "Error: no ReadCapacityUnits on globalIndex #{index.name}, going to next"
						state = STATE_ERROR
					end
					if index[:provisioned_throughput].include?(:write_capacity_units) 
					    update_index.capacity.currentWriteUnits = index[:provisioned_throughput][:write_capacity_units]
					else 
					    myPuts "Error: no WriteCapacityUnits on globalIndex #{index.name}, going to next"
					    state = STATE_ERROR
					end
					break
				    end
				end
			    end
			end
		else
			myPuts "Error: no Table, going to next",true
			state = STATE_ERROR
		end
	rescue AWS::DynamoDB::Errors::ThrottlingException => e
		myPuts "Warning: Have been throttled: msg: #{e}"
		state = STATE_THROTTLED
	rescue Exception => e
		myPuts "Error Getting Table Description:#{e}"
		state = STATE_ERROR
	end
	return state, aws_status
end

#####################################################
def get_data_points(cwApi, aws_namespace, tableName, metric, since, period, dimensionType, globalIndexName)
	
	ret_code = false
	data_points = []
	dimension = []

	begin
		dimension.push( { :name=>"TableName", :value=>"#{tableName}"})
		if globalIndexName != nil
		    dimension.push( {:name=>"GlobalSecondaryIndexName",:value=>globalIndexName} )
		end
		if dimensionType != nil
		    dimension.push( {:name=>"Operation",:value=>dimensionType} )
		end
		pp dimension if @verbose == true
		stats = @cwApi.client.get_metric_statistics( options = {
			:metric_name => metric,
			:statistics => [ "Average","Minimum", "Maximum","Sum","SampleCount"],
			:start_time => (Time.now() - since).utc.iso8601,
			:end_time => Time.now().utc.iso8601,
			:period => period,
			:namespace => "#{aws_namespace}",
			:dimensions => dimension
			} )
		pp stats if @verbose == true
		ret_code =  true
		data_points = stats.datapoints
	rescue Exception => e
		myPuts "Error: getting amazon stats #{e}",true
	end
	return ret_code,data_points
end


#####################################################
def get_max_data_point_per_second(cwApi, aws_namespace, tableName, metric, since, period, dimensionType,globalIndexName)
	max_value = 0.0
	ret_code,data_points = get_data_points(cwApi, aws_namespace, tableName, metric, since, period, dimensionType, globalIndexName)
	if ret_code == true 
		if data_points.length == 0 
			myPuts "no data points" if @verbose == true
		else 	
			sorted_dps = data_points.sort { |a,b| a[:timestamp] <=> b[:timestamp] } 
			sorted_dps.each do | dp |
				pp dp if @verbose == true
				c1 = dp[:sum]/period.to_f
				max_value = c1 if c1 > max_value 

			end
		end
	end
	myPuts "max_value = #{max_value}" if @verbose == true
	return max_value
end

#####################################################
def updateUnits(dynamoApiClient, table_name, table)

	myPuts "++++++++ updating  #{table_name} ++++++++++++++++++++++++" if @verbose == true
	if table.state == STATE_ERROR || table.state == STATE_UNKNOWN
		myPuts "Error:  table_name state not ok, skipping" if @verbose == true
		
	else
		#if compareCurrentGtEqDesiredUnits( table) == true
		if compareCurrentToDesiredUnits( table) == true
			return
		end

		table.capacity.nextWriteUnits  = getNextIncreaseLevel(table.capacity.currentWriteUnits, table.capacity.desiredWriteUnits)
		table.capacity.nextReadUnits  = getNextIncreaseLevel(table.capacity.currentReadUnits, table.capacity.desiredReadUnits)
		myPuts "Setting readUnits from #{table.capacity.currentReadUnits} to #{table.capacity.nextReadUnits} desired #{table.capacity.desiredReadUnits}" if @verbose == true
		myPuts "Setting writeUnits from #{table.capacity.currentWriteUnits} to #{table.capacity.nextWriteUnits} desired #{table.capacity.desiredWriteUnits}" if @verbose == true
		table.globalIndexes.each do | index |
		    index.capacity.nextWriteUnits  = getNextIncreaseLevel(index.capacity.currentWriteUnits, index.capacity.desiredWriteUnits)
		    index.capacity.nextReadUnits  = getNextIncreaseLevel(index.capacity.currentReadUnits, index.capacity.desiredReadUnits)
		    myPuts "Setting globalIndex #{index.name} readUnits from #{table.capacity.currentReadUnits} to #{table.capacity.nextReadUnits} desired #{table.capacity.desiredReadUnits}" if @verbose == true
		    myPuts "Setting globalIndex #{index.name} writeUnits from #{table.capacity.currentWriteUnits} to #{table.capacity.nextWriteUnits} desired #{table.capacity.desiredWriteUnits}" if @verbose == true
		end
			
		begin
			my_options = {}
			my_options[:table_name]=table_name
			if  (table.capacity.currentWriteUnits == table.capacity.desiredWriteUnits) &&
			    (table.capacity.currentReadUnits == table.capacity.desiredReadUnits) 
			    myPuts "Table units at required setting" if @verbose == true
			else
			    my_options[:provisioned_throughput] = { 
				:read_capacity_units=>table.capacity.nextReadUnits,
				:write_capacity_units=>table.capacity.nextWriteUnits }
			end
			if table.globalIndexes.length > 0 
			    table.globalIndexes.each do | index |
				if  (index.capacity.currentWriteUnits == index.capacity.desiredWriteUnits) &&
				    (index.capacity.currentReadUnits == index.capacity.desiredReadUnits) 
				    myPuts "globalIndex #{index.name}  units at required setting" if @verbose == true
				else
				    my_update = { 
					:update=> { 
					    :index_name=>index.name,
					    :provisioned_throughput=>{
						:read_capacity_units=>index.capacity.nextReadUnits,
						:write_capacity_units=>index.capacity.nextWriteUnits,
					    }
					}
				    }
				    if my_options.include?(:global_secondary_index_updates) == false
					my_options[:global_secondary_index_updates] = []
				    end
				    my_options[:global_secondary_index_updates].push(my_update)
				end
								
			    end


			end

			pp my_options if @verbose == true

			dyn_update = @dynamoApiClient.update_table( 
				options=my_options
				)

			myPuts "+ update+++++++++++++++++++++++++++++++" if @verbose == true
			pp dyn_update if @verbose == true
		rescue AWS::DynamoDB::Errors::LimitExceededException => e
			#if e.message.include?("Only 10 tables per subscriber can be created/updated/deleted simultaneously")
			#
			# Note: Not checked the complete message since amazon documentation says that we should be able to 
			#       do 20 but message states only 10
			#
			if e.message.include?("updated") && e.message.include?("simultaneously")
				myPuts "Warning Limit Exceeded for update, waiting to retry; #{e}" 
			elsif e.message.include?("Provisioned throughput can be decreased only")
				myPuts "Error Limit Exceeded for decrease; #{e}"
				table.state = STATE_ERROR
			else
				myPuts "Warning Limit Exceeded; #{e}"
				#table.state = STATE_ERROR
			end
		rescue AWS::DynamoDB::Errors::ThrottlingException => e
			myPuts "Warning: Have been throttled: msg: #{e}"
		rescue Exception => e
			myPuts "Error update:#{e}"
			table.state = STATE_ERROR
		end
	end
end

#####################################################
def getNextIncreaseLevel(current, desired)

        if desired > current
                if (current*2) < desired
                        return current*2
                else
                        return desired
                end

        else
                return desired
        end

end
#####################################################
def display_menu
    puts "Usage: #{$0} [-v]"
    puts "  --help, -h:                                 This Help"
    puts "  --test, -x:                                 Enable test mode"
    puts "  --verbose, -v:                              Enable verbose mode"
    puts "  --list_regions, -l:                         List the amazon region endpoints"
    puts "  --log_output <id>, -p <id>:                 Log output to with id/timestamp in dir #{@log_dir}"
	puts "  --overrides_file <file> , -n <file>:        Json format file to specify cmd line options"
    puts "  --config_file <file> , -c <file>:           yaml configuration file of tables to query "
    puts "  --ignore_file <file> , -i <file>:           yaml configuration file of tables to ignore "
    puts "  --table <table_name>, -t <table_name>:      Table to query"
    puts "  --region <aws region>, -r <aws region>:     Amazon region.  Default:#{@dynamo_db_endpoint}"
	puts "  --frequency <seconds>, -q <seconds>:        Check the table throughputs every N seconds"
    puts "  "
    puts "  Note: --config_file or --table must be specified"

    exit
end
#####################################################
def display_regions
        puts "Amazon Endpoints:"

        @dynamo_regions.each do |key, name|
            puts "Location '#{key}' Region '#{name}'"
        end
        exit
end

#####################################################
def validate_region( regionIn, dynamo_regions)
	isValidRegion = false
	dynamo_regions.each do |key, name|
		if regionIn == name
			isValidRegion = true
		end
	end
    return isValidRegion
end

#####################################################
##### main
#####################################################
opts = GetoptLong.new

# add options
opts.set_options(
        [ "--help", "-h", GetoptLong::NO_ARGUMENT ], \
        [ "--verbose", "-v", GetoptLong::NO_ARGUMENT ], \
        [ "--test", "-x", GetoptLong::NO_ARGUMENT ], \
        [ "--list_regions", "-l", GetoptLong::NO_ARGUMENT ], \
        [ "--log_output", "-p", GetoptLong::REQUIRED_ARGUMENT ], \
        [ "--table", "-t", GetoptLong::REQUIRED_ARGUMENT ], \
		[ "--overrides_file", "-n", GetoptLong::REQUIRED_ARGUMENT ], \
		[ "--config_file", "-c", GetoptLong::REQUIRED_ARGUMENT ], \
		[ "--ignore_file", "-i", GetoptLong::REQUIRED_ARGUMENT ], \
		[ "--output_file", "-o", GetoptLong::REQUIRED_ARGUMENT ], \
		[ "--frequency", "-q", GetoptLong::REQUIRED_ARGUMENT ], \
        [ "--region", "-r", GetoptLong::REQUIRED_ARGUMENT ]
      )

@cmdline_table = nil

# parse options
begin
	opts.each { |opt, arg|
	  case opt
	    when '--overrides_file'
		  # read the json file
		  jsonFile = File.read(arg)
		  overrides_hash = JSON.parse(jsonFile)
		  @verbose = overrides_hash['verbose']
		  @frequency = overrides_hash['frequency']
		  @config_file = overrides_hash['configFile']
		  @ignore_file = overrides_hash['ignoreFile']
		  @secret_access_key = overrides_hash['secret_access_key']
		  @access_key_id = overrides_hash['access_key_id']
		  if validate_region( overrides_hash['region'],@dynamo_regions)
		      @dynamo_db_endpoint = overrides_hash['region']
		  else
		      puts "Error: Invalid region specified in the config file. Region must be one of..."
			  display_regions
		  end
	    when '--help'
	      display_menu
	    when '--test'
		  @test_mode = true
	    when '--verbose'
	      @verbose = true
		when '--frequency'
		  @frequency = arg
	    when '--log_output'
		  @log_output = true
		  @log_output_id = arg
		  @log_file_name = "dynamo_cloudwatch_#{@log_output_id}_" + @start_time.strftime("%Y-%m-%d") + ".log"
        when '--list_regions'
          display_regions
	    when '--config_file'
          @config_file = arg
	    when '--ignore_file'
          @ignore_file = arg
	    when '--output_file'
          @output_file = arg
	    when '--table'
		  @cmdline_table = arg
        when '--region'
		  if validate_region( arg,@dynamo_regions)
	        @dynamo_db_endpoint = arg
		  else
		    puts "Error: Invalid region specified on the command line. Region must be one of..."
			display_regions
		  end
	  end
	}
	
rescue => err
        #puts "#{err.class()}: #{err.message}"
        display_menu
end

# See if there are any environment specific overrides
if ENV['VERBOSE']
	myPuts "Verbose specified in environment - enabling verbose logging",true
	@verbose = true
end

if ENV['DYNAMODB_REGION']
	myPuts "DynamoDB region specified in environment - #{ENV['DYNAMODB_REGION']}",true
	if validate_region( ENV['DYNAMODB_REGION'],@dynamo_regions)
		@dynamo_db_endpoint = ENV['DYNAMODB_REGION']
	else
		puts "Error: Invalid region specified in the environment. Region must be one of..."
		display_regions
	end
end

if ENV['FREQUENCY']
	myPuts "Polling frequency specified in environment - #{ENV['FREQUENCY']}",true
	@frequency = ENV['FREQUENCY']
end

if @cmdline_table == nil && @config_file == nil
	myPuts "require --table or --config_file to be specified"
	display_menu
end
if @cmdline_table != nil
	@dynamodb_tables[@cmdline_table]  = Dynamodb_table.new(@cmdline_table)
end

@st_measurement = Time.now()

if @config_file != nil
    config_read = false
    while config_read == false do
	    @return_code,@service_output =  read_config(@config_file)
	    if @return_code != 0
		    myPuts "Error reading config - does the config file exist yet? #{@config_file}, msg #{@service_output}",true
		    #myPuts "#{@service_output}|#{@service_perfdata}",true
			sleep 10
		    # exit @return_code
		else
		    config_read = true
	    end
	end 
end

if @ignore_file != nil
        if  File.exists?(@ignore_file) 
		@return_code,@service_output =  read_ignore(@ignore_file)
		if @return_code != 0
			myPuts "Error reading ignore #{@ignore_file}, msg #{@service_output}",true
			#myPuts "#{@service_output}|#{@service_perfdata}",true
			# exit @return_code
		end
	else
		myPuts "ignore file does not exist",false
	end
end

# DJN loop here
while true do
	
	# connect to Amazon
	d = DateTime.now
	myPuts "Process starts at #{d}",true
	myPuts "connecting to AWS dynamoDB region #{@dynamo_db_endpoint}",true
	
	useKeys = false
	if @access_key_id != nil && @access_key_id.length > 0
	  useKeys = true
	end
	
	begin
	  if useKeys
	    # use aws credentials in the overrides file
		myPuts "Using AWS credentials in overrides file to connect to DynamoDB",true
	    AWS.config(:dynamo_db => {:api_version => '2012-08-10'},
					:access_key_id => @access_key_id, 
					:secret_access_key => @secret_access_key,
					:dynamo_db_endpoint => @dynamo_db_endpoint)
	  else
	    # use role based credentials
		myPuts "Using AWS role credentials to connect to DynamoDB",true
	    AWS.config(:dynamo_db => {:api_version => '2012-08-10'},
					:dynamo_db_endpoint => @dynamo_db_endpoint)
	  end
	  
	  @dynamoApiClient = AWS::DynamoDB::Client.new

	rescue Exception => e
	  myPuts "Error occured while trying to connect to DynamoDB endpoint: #{e}",true
	  exit CODE_CRITICAL
	end

	@cwApi = nil
	begin
	  if useKeys
	    myPuts "Using AWS credentials in overrides file to connect to CloudWatch",true
	    @cwApi = AWS::CloudWatch.new( :access_key_id => @access_key_id, :secret_access_key => @secret_access_key)
	  else
	    myPuts "Using AWS role credentials to connect to CloudWatch",true
	    @cwApi = AWS::CloudWatch.new
	  end
	rescue Exception => e
	  myPuts "Error occured while trying to connect to Cloudwatch endpoint: #{e}",true
	  exit CODE_CRITICAL
	end

	#########
	### load up the tables that need to be changed
	#########

	@is_truncated = true
	@exclusive_start_table_name = nil
	while @is_truncated == true

		begin
			if @exclusive_start_table_name.nil?
				tables = @dynamoApiClient.list_tables()
			else 
				tables = @dynamoApiClient.list_tables( options={:exclusive_start_table_name=>@exclusive_start_table_name})
			end
		rescue Exception => e
			myPuts "Error unable to start process with Amazon; aborting process",true
			myPuts "Error List Tables:#{e}",true
			exit! CODE_CRITICAL
		end
		#pp tables if @verbose == true
		@dynamodb_tables.each do | table_name, table|
			if tables[:table_names].include?(table_name)
				table.state = STATE_OK
				state, aws_status  = getCurrentUnits(@dynamoApiClient, table_name, table)
				table.state = state
				myPuts "Checking table: #{table_name}",@std_out_print
				if state == STATE_OK
					table.aws_status = aws_status
					table.capacity.originalReadUnits = table.capacity.currentReadUnits
					table.capacity.originalWriteUnits = table.capacity.currentWriteUnits

					table.capacity.currentCloudWatchReadUnits = get_max_data_point_per_second(@cwApi, DYNAMODB_NAMESPACE, table_name, "ConsumedReadCapacityUnits", @cloudwatch_timer, CLOUDWATCH_PERIOD,nil,nil)
					table.capacity.currentCloudWatchWriteUnits = get_max_data_point_per_second(@cwApi, DYNAMODB_NAMESPACE, table_name, "ConsumedWriteCapacityUnits", @cloudwatch_timer, CLOUDWATCH_PERIOD,nil,nil)

					table.globalIndexes.each do | index |
						index.capacity.originalReadUnits = index.capacity.currentReadUnits
						index.capacity.originalWriteUnits = index.capacity.currentWriteUnits
						index.capacity.currentCloudWatchReadUnits = get_max_data_point_per_second(@cwApi, DYNAMODB_NAMESPACE, table_name, "ConsumedReadCapacityUnits", @cloudwatch_timer, CLOUDWATCH_PERIOD,nil,index.name)
						index.capacity.currentCloudWatchWriteUnits = get_max_data_point_per_second(@cwApi, DYNAMODB_NAMESPACE, table_name, "ConsumedWriteCapacityUnits", @cloudwatch_timer, CLOUDWATCH_PERIOD,nil,index.name)
					end

					if @verbose == true
						table.throttledRequestsScan = get_max_data_point_per_second(@cwApi, DYNAMODB_NAMESPACE, table_name, "ThrottledRequests", @cloudwatch_timer, CLOUDWATCH_PERIOD,"Scan",nil)
						table.systemErrorsScan = get_max_data_point_per_second(@cwApi, DYNAMODB_NAMESPACE, table_name, "SystemErrors", @cloudwatch_timer, CLOUDWATCH_PERIOD,"Scan",nil)

						table.throttledRequestsQuery = get_max_data_point_per_second(@cwApi, DYNAMODB_NAMESPACE, table_name, "ThrottledRequests", @cloudwatch_timer, CLOUDWATCH_PERIOD,"Query",nil)
						table.systemErrorsQuery = get_max_data_point_per_second(@cwApi, DYNAMODB_NAMESPACE, table_name, "SystemErrors", @cloudwatch_timer, CLOUDWATCH_PERIOD,"Query",nil)

						table.throttledRequestsUpdateItem = get_max_data_point_per_second(@cwApi, DYNAMODB_NAMESPACE, table_name, "ThrottledRequests", @cloudwatch_timer, CLOUDWATCH_PERIOD,"UpdateItem",nil)
						table.systemErrorsUpdateItem = get_max_data_point_per_second(@cwApi, DYNAMODB_NAMESPACE, table_name, "SystemErrors", @cloudwatch_timer, CLOUDWATCH_PERIOD,"UpdateItem",nil)
					end
				end
			end
		end
		if tables.include?(:last_evaluated_table_name)
			myPuts "-----------------------------------",@std_out_print
			@exclusive_start_table_name = tables[:last_evaluated_table_name]
		else
			@is_truncated = false
		end
	end

	pp @dynamodb_tables if @verbose == true

	#myPuts Time.now.strftime("%Y-%m-%d-%H:%M:%S") 
	@dynamodb_tables.each do |table_name, table|
		myPuts "#{table_name} #{table.aws_status}",@std_out_print
		if table.state == STATE_OK
			myPuts "     readUnits consumed #{table.capacity.currentCloudWatchReadUnits} of #{table.capacity.currentReadUnits}",@std_out_print
			myPuts "     writeUnits consumed #{table.capacity.currentCloudWatchWriteUnits} of #{table.capacity.currentWriteUnits}",@std_out_print
			table.globalIndexes.each do | index |
				myPuts "     globalSecIndex #{index.name} readUnits consumed #{index.capacity.currentCloudWatchReadUnits} of #{index.capacity.currentReadUnits}",@std_out_print
				myPuts "     globalSecIndex #{index.name} writeUnits consumed #{index.capacity.currentCloudWatchWriteUnits} of #{index.capacity.currentWriteUnits}",@std_out_print
			end
			if @verbose == true
				myPuts "     Scan: throttled #{table.throttledRequestsScan} systemErrors #{table.systemErrorsScan}",@std_out_print
				myPuts "     Query: throttled #{table.throttledRequestsQuery} systemErrors #{table.systemErrorsQuery}",@std_out_print
				myPuts "     UpdateItem: throttled #{table.throttledRequestsUpdateItem} systemErrors #{table.systemErrorsUpdateItem}",@std_out_print
			end
			if table.aws_status == DYNAMODB_STATUS_ACTIVE 
				if (table.capacity.currentCloudWatchReadUnits / table.capacity.currentReadUnits.to_f) > @threshold_over
					myPuts " increase #{table_name} readUnits #{table.capacity.currentCloudWatchReadUnits} of #{table.capacity.currentReadUnits}"

					increaseBy = (table.capacity.currentReadUnits * (table.capacity.increaseReadUnitsByPercentage/100.0)).round
					table.capacity.desiredReadUnits = table.capacity.currentReadUnits + increaseBy

					##### table.capacity.desiredReadUnits = Integer(table.capacity.currentReadUnits * @default_threshold_increase)
					@dynamodb_tables_to_update[table_name] = table
					
				end
				if (table.capacity.currentCloudWatchWriteUnits / table.capacity.currentWriteUnits.to_f) > @threshold_over
					myPuts " increase #{table_name} writeUnits #{table.capacity.currentCloudWatchWriteUnits} of #{table.capacity.currentWriteUnits}"

					increaseBy = (table.capacity.currentWriteUnits * (table.capacity.increaseWriteUnitsByPercentage/100.0)).round
					table.capacity.desiredWriteUnits = table.capacity.currentWriteUnits + increaseBy

					##### table.capacity.desiredWriteUnits = Integer(table.capacity.currentWriteUnits * @default_threshold_increase)
					@dynamodb_tables_to_update[table_name] = table
				end
				table.globalIndexes.each do | index |
					if index.index_status == DYNAMODB_STATUS_ACTIVE 
					if (index.capacity.currentCloudWatchReadUnits / index.capacity.currentReadUnits.to_f) > @threshold_over
						myPuts " increase #{table_name} globalSecIndex #{index.name} readUnits #{index.capacity.currentCloudWatchReadUnits} of #{index.capacity.currentReadUnits}"

						increaseBy = (index.capacity.currentReadUnits * (index.capacity.increaseReadUnitsByPercentage/100.0)).round
						index.capacity.desiredReadUnits = index.capacity.currentReadUnits + increaseBy

						@dynamodb_tables_to_update[table_name] = table
						
					end
					if (index.capacity.currentCloudWatchWriteUnits / index.capacity.currentWriteUnits.to_f) > @threshold_over
						myPuts " increase #{table_name} globalSecIndex #{index.name} writeUnits #{index.capacity.currentCloudWatchWriteUnits} of #{index.capacity.currentWriteUnits}"

						increaseBy = (index.capacity.currentWriteUnits * (index.capacity.increaseWriteUnitsByPercentage/100.0)).round
						index.capacity.desiredWriteUnits = index.capacity.currentWriteUnits + increaseBy

						@dynamodb_tables_to_update[table_name] = table
					end
					end
				end
			else
				myPuts "     Warning: Table is not active so will not evaluate for increase",@std_out_print
			end
		else
			myPuts "     Warning: Table not found in dynamodb",@std_out_print
		end
	end

	#### 
	#### state which tables will be updated
	####
	if @dynamodb_tables_to_update.length > 0
		@service_perfdata = "Tables: "
		@service_output = "Tables updated: "
		@rc = CODE_WARNING
	else
		@service_perfdata = ""
		@service_output = "OK"
		@rc = CODE_OK
	end

	#### 
	#### validate the data for update
	####
	@dynamodb_tables_to_update.each do |table_name, table_to_update|
		if table_to_update.capacity.desiredReadUnits == nil
			table_to_update.capacity.desiredReadUnits = table_to_update.capacity.currentReadUnits
		else
			myPuts " change #{table_name} readUnits #{table_to_update.capacity.currentReadUnits} to #{table_to_update.capacity.desiredReadUnits}"
		end
		if table_to_update.capacity.desiredWriteUnits == nil
			table_to_update.capacity.desiredWriteUnits = table_to_update.capacity.currentWriteUnits
		else
			myPuts " change #{table_name} writeUnits #{table_to_update.capacity.currentWriteUnits} to #{table_to_update.capacity.desiredWriteUnits}"
		end
		table_to_update.globalIndexes.each do | index |
			if index.capacity.desiredReadUnits == nil
				index.capacity.desiredReadUnits = index.capacity.currentReadUnits
			else
				myPuts " change #{table_name} globalSecIndex #{index.name} readUnits #{index.capacity.currentReadUnits} to #{index.capacity.desiredReadUnits}"
			end
			if index.capacity.desiredWriteUnits == nil
				index.capacity.desiredWriteUnits = index.capacity.currentWriteUnits
			else
				myPuts " change #{table_name} globalSecIndex #{index.name} writeUnits #{index.capacity.currentWriteUnits} to #{index.capacity.desiredWriteUnits}"
			end
		end
	end

	if @test_mode == true
		myPuts " Test mode enabled - will not modify any tables.",true
		exit
	end

	#########
	### set the tables that need to be changed
	#########
	while @dynamodb_tables_to_update.length > 0

			@dynamodb_tables_to_update.each do |table_name, table_to_update|
					if table_to_update.state == STATE_ERROR || table_to_update.state == STATE_UNKNOWN
							myPuts "Error table #{table_name} state in error: #{table_to_update.state}",true
							@dynamodb_tables_to_update.delete(table_name)
					end

			state, status  = getCurrentUnits(@dynamoApiClient, table_name, table_to_update)
					if state == STATE_ERROR
							myPuts "Error table #{table_name} get units failed state: #{state}",true
							@dynamodb_tables_to_update.delete(table_name)
					end
					case status
							when DYNAMODB_STATUS_CREATING
									myPuts "table #{table_name} aws_status CREATING, waiting for table to be status of active",@std_out_print
							when DYNAMODB_STATUS_UPDATING
									myPuts "table #{table_name} aws_status UPDATING readUnits #{table_to_update.capacity.nextReadUnits} writeUnits #{table_to_update.capacity.nextWriteUnits}, waiting..."
							when DYNAMODB_STATUS_ACTIVE
					global_index_status_active = true
						table_to_update.globalIndexes.each do | index |
						if index.index_status != DYNAMODB_STATUS_ACTIVE
						global_index_status_active = false
						myPuts "table #{table_name} globalIndex #{index.name} status #{index.index_status}, waiting..."
						break
						end
					end
					if global_index_status_active == false
						next
					end

									#if  compareCurrentGtEqDesiredUnits(table_to_update)
									if  compareCurrentToDesiredUnits(table_to_update)
						update_description = ""
						if  table_to_update.capacity.originalReadUnits != table_to_update.capacity.currentReadUnits 
							update_description = "read #{table_to_update.capacity.originalReadUnits}->#{table_to_update.capacity.currentReadUnits}" 
						end
						if  table_to_update.capacity.originalWriteUnits != table_to_update.capacity.currentWriteUnits 
							update_description = update_description + " write #{table_to_update.capacity.originalWriteUnits}->#{table_to_update.capacity.currentWriteUnits}" 
						end
						table_to_update.globalIndexes.each do | index |
							global_description = ""
							if  index.capacity.originalReadUnits != index.capacity.currentReadUnits 
								global_description = " read #{index.capacity.originalReadUnits}->#{index.capacity.currentReadUnits}" 
							end
							if  index.capacity.originalWriteUnits != index.capacity.currentWriteUnits 
								global_description = global_description + "  write #{index.capacity.originalWriteUnits}->#{index.capacity.currentWriteUnits}" 
							end
							if global_description != ""
							update_description = update_description + " globalSecIndex #{index.name}->{" + global_description + "} "
							end 
						end
											myPuts "Table #{table_name} Updated",@std_out_print
						@service_table_list = @service_table_list + " #{table_name}(OK #{update_description}),"
											@dynamodb_tables_to_update.delete(table_name)
											next
									end
									myPuts "table #{table_name} aws_status = active; checking to see if need to update table" if @verbose == true
									updateUnits( @dynamoApiClient, table_name, table_to_update)
									if table_to_update.state == STATE_ERROR
											myPuts "Error update table #{table_name} failed",true
						@service_table_list = @service_table_list + " #{table_name}(FAILED),"
											@dynamodb_tables_to_update.delete(table_name)
											next
									end
							else
									myPuts "Error: unknown status #{status}",true
									@dynamodb_tables_to_update.delete(table_name)
		
									next
					end
			end
			if  @dynamodb_tables_to_update.length > 0
					sleep 5
			end
	end

	@delta_measurement = Time.now() - @st_measurement
	@service_perfdata =  "sampleDate = " + Time.now.to_s + "; execution time= " +"%10.5f" % @delta_measurement.to_f + "; #{@service_table_list}"

	#myPuts "#{@service_output}#{@service_table_list}|#{@service_perfdata}",true
	myPuts "#{@service_output}#{@service_table_list}",true

	d = DateTime.now
	myPuts "Finished evaluating tables at #{d}",true
	e = d + Rational(@frequency,86400)
	myPuts "Will wake again at #{e}",true
	sleep @frequency

end #infinite while loop

exit @rc
