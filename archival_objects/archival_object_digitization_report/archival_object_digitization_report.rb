class ArchivalObjectDigitizationReport < AbstractReport

  register_report

  def headers
    ['title', 'component_id', 'location', 'url']
  end

  def query
    repo_id = @params.fetch(:repo_id)

    locations = db[:external_document]
      .select(Sequel.as(:archival_object_id, :id),
              Sequel.as(:location, :location))
      .where(:title => "Special Collections @ DU")

    digital_objects = db[:instance]
      .left_outer_join(:instance_do_link_rlshp, :instance_id => :instance__id)
      .left_outer_join(:digital_object, :id => :instance_do_link_rlshp__digital_object_id)
      .select(Sequel.as(:instance__archival_object_id, :id),
              Sequel.as(:digital_object__digital_object_id, :url))

    db[:archival_object]
      .left_outer_join(locations, {:id => :archival_object__id}, :table_alias => :location)
      .left_outer_join(digital_objects, {:id => :archival_object__id}, :table_alias => :digital_object)
      .select(Sequel.as(:archival_object__title, :title),
              Sequel.as(:archival_object__component_id, :component_id),
              Sequel.as(:location__location, :location),
              Sequel.as(:digital_object__url, :url))
      .filter(:archival_object__repo_id => repo_id)
      .exclude(:location__location => nil)
  end

end
