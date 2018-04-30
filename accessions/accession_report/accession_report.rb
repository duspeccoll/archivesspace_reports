class AccessionReport < AbstractReport

  register_report

  def headers
    ['id', 'identifier', 'title', 'accession_date', 'description', 'inventory', 'resources']
  end

  def processor
    {
      'identifier' => proc {|record| parse_identifier(record[:accession_identifier])}
    }
  end

  def query
    repo_id = @params[:repo_id]

    related_resources = db[:spawned_rlshp]
      .left_outer_join(:resource, :id => :spawned_rlshp__resource_id)
      .select(Sequel.as(:spawned_rlshp__accession_id, :accession_id))
      .select_more{Sequel.as(group_concat(replace(replace(:resource__identifier, '["',''),'",null,null,null]','')), :resources)}
      .group_by(:spawned_rlshp__accession_id)

    db[:accession]
      .left_outer_join(related_resources, {:accession_id => :accession__id}, :table_alias => :related_resources)
      .select(Sequel.as(:accession__id, :id),
              Sequel.as(:accession__identifier, :accession_identifier),
              Sequel.as(:accession__title, :title),
              Sequel.as(:accession__accession_date, :accession_date),
              Sequel.as(:accession__content_description, :description),
              Sequel.as(:accession__inventory, :inventory),
              Sequel.as(:related_resources__resources, :resources))
      .filter(:accession__repo_id => repo_id)
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
