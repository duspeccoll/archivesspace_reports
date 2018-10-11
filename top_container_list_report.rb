class TopContainerListReport < AbstractReport

  register_report

  def query_string
    "select
      top_container.barcode as barcode,
      top_container.indicator as indicator,
      container_profile.name as profile_name,
      location.title as location
    from top_container
      left outer join top_container_profile_rlshp
        on top_container_profile_rlshp.top_container_id = top_container.id
      left outer join container_profile
        on container_profile.id = top_container_profile_rlshp.container_profile_id
      left outer join top_container_housed_at_rlshp
        on top_container_housed_at_rlshp.top_container_id = top_container.id
      left outer join location
        on location.id = top_container_housed_at_rlshp.location_id
    where top_container.repo_id = #{db.literal(@repo_id)}"
  end

end
