class ArchivalObjectDigitizationReport < AbstractReport

  register_report

  def headers
    ['uri', 'title', 'component_id', 'location', 'url', 'is_representative']
  end

  def processor
    {
      'is_representative' => proc {|record| record[:representative].nil? ? false : true}
    }
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
              Sequel.as(:digital_object__digital_object_id, :url),
              Sequel.as(:instance__is_representative, :is_representative))

    db[:archival_object]
      .left_outer_join(locations, {:id => :archival_object__id}, :table_alias => :location)
      .left_outer_join(digital_objects, {:id => :archival_object__id}, :table_alias => :digital_object)
      .select(Sequel.as(Sequel.lit("concat('/repositories/2/archival_objects/',archival_object.id)"), :uri),
              Sequel.as(:archival_object__title, :title),
              Sequel.as(:archival_object__component_id, :component_id),
              Sequel.as(:location__location, :location),
              Sequel.as(:digital_object__url, :url),
              Sequel.as(:digital_object__is_representative, :representative))
      .filter(:archival_object__repo_id => repo_id)
      .exclude(:location__location => nil)
  end

end
