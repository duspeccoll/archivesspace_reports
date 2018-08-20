class AccessionReport < AbstractReport

  register_report

  def headers
    ['id', 'title', 'identifier', 'created_by', 'created_at', 'accession_date', 'description', 'inventory', 'linear_feet', 'items', 'resources']
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

    linear_feet = db[:extent]
      .left_outer_join(:enumeration_value, :id => :extent__extent_type_id)
      .select(Sequel.as(:extent__accession_id, :accession_id),
              Sequel.as(Sequel.lit("concat(extent.number, ' ', replace(enumeration_value.value, '_', ' '))"), :extent))
      .exclude(:extent__accession_id => nil)
      .where(:enumeration_value__value => 'linear_feet')

    items = db[:extent]
      .left_outer_join(:enumeration_value, :id => :extent__extent_type_id)
      .select(Sequel.as(:extent__accession_id, :accession_id),
              Sequel.as(Sequel.lit("concat(extent.number, ' ', replace(enumeration_value.value, '_', ' '))"), :extent))
      .exclude(:extent__accession_id => nil)
      .where(:enumeration_value__value => 'items')

    db[:accession]
      .left_outer_join(related_resources, {:accession_id => :accession__id}, :table_alias => :related_resources)
      .left_outer_join(linear_feet, {:accession_id => :accession__id}, :table_alias => :linear_feet)
      .left_outer_join(items, {:accession_id => :accession__id}, :table_alias => :items)
      .select(Sequel.as(:accession__id, :id),
              Sequel.as(:accession__title, :title),
              Sequel.as(:accession__identifier, :accession_identifier),
              Sequel.as(:accession__created_by, :created_by),
              Sequel.as(:accession__create_time, :created_at),
              Sequel.as(:accession__accession_date, :accession_date),
              Sequel.as(:accession__content_description, :description),
              Sequel.as(:accession__inventory, :inventory),
              Sequel.as(:linear_feet__extent, :linear_feet),
              Sequel.as(:items__extent, :items),
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
