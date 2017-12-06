class ArchivalObjectProcessingNotesReport < AbstractReport

  register_report

  def headers
    ['call_number', 'title', 'processing_note']
  end

  def query
    repo_id = @params[:repo_id]

    db[:archival_object]
    	.select(Sequel.as(:component_id, :call_number),
    		Sequel.as(:title, :title),
    		Sequel.as(:repository_processing_note, :processing_note))
    	.filter(:repo_id => repo_id)
    	.exclude(:repository_processing_note => nil)
      .order(Sequel.asc(:call_number))
   end

end
