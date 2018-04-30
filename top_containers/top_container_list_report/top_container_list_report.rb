class TopContainerListReport < AbstractReport

  register_report

  def headers
    ['barcode', 'indicator', 'profile_name', 'location']
  end

  def query
    repo_id = @params.fetch(:repo_id)

    db[:top_container]
      .left_outer_join(:top_container_profile_rlshp, :top_container_id => :top_container__id)
      .left_outer_join(:container_profile, :id => :top_container_profile_rlshp__container_profile_id)
      .left_outer_join(:top_container_housed_at_rlshp, :top_container_id => :top_container__id)
      .left_outer_join(:location, :id => :top_container_housed_at_rlshp__location_id)
      .filter(:top_container__repo_id => repo_id)
      .select(Sequel.as(:top_container__barcode, :barcode),
             Sequel.as(:top_container__indicator, :indicator),
             Sequel.as(:container_profile__name, :profile_name),
             Sequel.as(:location__title, :location)
      )
  end

end
