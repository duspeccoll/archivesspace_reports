class ResourceProcessingNotesReport < AbstractReport

  register_report

  def headers
    ['call_number', 'title', 'processing_note']
  end

  def processor
  	{
  		'call_number' => proc {|record| parse_identifier(record[:type], record[:identifier])}
  	}
  end

  def query
    repo_id = @params[:repo_id]

    resources = db[:resource]
    	.select(Sequel.as(:identifier, :identifier),
              Sequel.as('resource', :type),
    					Sequel.as(:title, :title),
    					Sequel.as(:repository_processing_note, :processing_note))
    	.filter(:repo_id => repo_id)
    	.exclude(:repository_processing_note => nil)

    archival_objects = db[:archival_object]
    	.select(Sequel.as(:component_id, :identifier),
              Sequel.as('archival_object', :type),
    		      Sequel.as(:title, :title),
    		      Sequel.as(:repository_processing_note, :processing_note))
    	.filter(:repo_id => repo_id)
    	.exclude(:repository_processing_note => nil)

    resources
      .union(archival_objects)
   end

   private

   def parse_identifier(type, s)
   	if ASUtils.blank?(s)
   		s
   	else
      if type == 'resource'
   		   ASUtils.json_parse(s).compact[0]
      else
        s
      end
    end
  end

end
