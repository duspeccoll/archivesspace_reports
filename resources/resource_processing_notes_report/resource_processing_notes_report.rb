class ResourceProcessingNotesReport < AbstractReport

  register_report

  def headers
    ['call_number', 'title', 'processing_note']
  end

  def processor
  	{
  		'call_number' => proc {|record| parse_identifier(record[:identifier])}
  	}
  end

  def query
    repo_id = @params[:repo_id]

    db[:resource]
    	.select(Sequel.as(:identifier, :identifier),
    					Sequel.as(:title, :title),
    					Sequel.as(:repository_processing_note, :processing_note))
    	.filter(:repo_id => repo_id)
    	.exclude(:repository_processing_note => nil)
      .order(Sequel.asc(:identifier))
   end

   private

   def parse_identifier(s)
   	if ASUtils.blank?(s)
   		s
   	else
   		id = ASUtils.json_parse(s).compact[0]
    end
  end

end
