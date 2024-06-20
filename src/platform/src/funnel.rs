use std::collections::HashMap;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use common::GROUPS_COUNT;
use common::rbac::ProjectPermission;
use metadata::MetadataProvider;
use query::context::Format;
use query::funnel::FunnelProvider;
use crate::{Breakdown, Context, FunnelResponse, FunnelStep, FunnelStepData, PlatformError, QueryParams, QueryResponseFormat, QueryTime, validate_event, validate_event_filter, validate_event_property};
use crate::queries::funnel;
use crate::queries::funnel::{ExcludeSteps, Filter, FunnelRequest, StepOrder};

pub struct Funnel {
    md: Arc<MetadataProvider>,
    prov: Arc<FunnelProvider>,
}

impl Funnel {
    pub fn new(md: Arc<MetadataProvider>, prov: Arc<FunnelProvider>) -> Self {
        Self { md, prov }
    }

    pub async fn funnel(
        &self,
        ctx: Context,
        project_id: u64,
        req: FunnelRequest,
        query: QueryParams,
    ) -> crate::Result<FunnelResponse> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        validate_request(&self.md, project_id, &req)?;
        let req = fix_request(req.into())?;

        let lreq = req.into();
        let cur_time = match query.timestamp {
            None => Utc::now(),
            Some(ts_sec) => DateTime::from_naive_utc_and_offset(
                chrono::NaiveDateTime::from_timestamp_millis(ts_sec * 1000).unwrap(),
                Utc,
            ),
        };
        let ctx = query::Context {
            project_id,
            format: match &query.format {
                None => Format::Regular,
                Some(format) => match format {
                    QueryResponseFormat::Json => Format::Regular,
                    QueryResponseFormat::JsonCompact => Format::Compact,
                },
            },
            cur_time,
        };

        let mut qdata = self.prov.funnel(ctx, lreq).await?;

        let groups = qdata
            .groups;

        let steps = qdata
            .steps
            .iter()
            .map(|step| {
                let data = step
                    .data
                    .iter()
                    .map(|data| {
                        FunnelStepData {
                            groups: data.groups.clone(),
                            ts: data.ts.clone(),
                            total: data.total.clone(),
                            conversion_ratio: data.conversion_ratio.clone(),
                            avg_time_to_convert: data.avg_time_to_convert.clone(),
                            avg_time_to_convert_from_start: data.avg_time_to_convert_from_start.clone(),
                            dropped_off: data.dropped_off.clone(),
                            drop_off_ratio: data.drop_off_ratio.clone(),
                            time_to_convert: data.time_to_convert.clone(),
                            time_to_convert_from_start: data.time_to_convert_from_start.clone(),
                        }
                    })
                    .collect::<Vec<_>>();
                FunnelStep {
                    step: step.step.clone(),
                    data,
                }
            })
            .collect::<Vec<_>>();
        let resp = FunnelResponse { groups, steps };
        Ok(resp)
    }
}

pub(crate) fn validate_request(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: &FunnelRequest,
) -> crate::Result<()> {
    if req.group > GROUPS_COUNT - 1 {
        return Err(PlatformError::BadRequest(
            "group id is out of range".to_string(),
        ));
    }

    match req.time {
        QueryTime::Between { from, to } => {
            if from > to {
                return Err(PlatformError::BadRequest(
                    "from time must be less than to time".to_string(),
                ));
            }
        }
        _ => {}
    }

    if req.steps.is_empty() {
        return Err(PlatformError::BadRequest(
            "steps must not be empty".to_string(),
        ));
    }

    if req.steps.len() > 5 {
        return Err(PlatformError::BadRequest(
            "steps must not be more than 5".to_string(),
        ));
    }
    for (step_id, step) in req.steps.iter().enumerate() {
        if step.events.is_empty() {
            return Err(PlatformError::BadRequest(format!(
                "step #{step_id}, events must not be empty"
            )));
        }

        for (event_id, event) in step.events.iter().enumerate() {
            validate_event(
                md,
                project_id,
                &event.event,
                event_id,
                format!("step #{step_id}, "),
            )?;

            match &event.filters {
                Some(filters) => {
                    for (filter_id, filter) in filters.iter().enumerate() {
                        validate_event_filter(
                            md,
                            project_id,
                            filter,
                            filter_id,
                            format!("event #{event_id}, "),
                        )?;
                    }
                }
                None => {}
            }
        }

        match &step.order {
            StepOrder::Exact => {}
            StepOrder::Any { steps } => steps
                .iter()
                .map(|(from, to)| {
                    if *from >= req.steps.len() {
                        return Err(PlatformError::BadRequest(
                            "step_order: from step index out of range".to_string(),
                        ));
                    }
                    if *to >= req.steps.len() {
                        return Err(PlatformError::BadRequest(
                            "step_order: to step index out of range".to_string(),
                        ));
                    }
                    Ok(())
                })
                .collect::<crate::Result<_>>()?,
        }
    }

    if let Some(exclude) = &req.exclude {
        for (exclude_id, exclude) in exclude.iter().enumerate() {
            validate_event(
                md,
                project_id,
                &exclude.event.event,
                exclude_id,
                format!("exclude #{exclude_id}, "),
            )?;
            match &exclude.steps {
                Some(steps) => match steps {
                    ExcludeSteps::Between { from, to } => {
                        if *from >= req.steps.len() {
                            return Err(PlatformError::BadRequest(
                                "exclude: from step index out of range".to_string(),
                            ));
                        }
                        if *to >= req.steps.len() {
                            return Err(PlatformError::BadRequest(
                                "exclude: to step index out of range".to_string(),
                            ));
                        }
                    }
                    ExcludeSteps::All => {}
                },
                None => {}
            }
        }
    }
    match &req.breakdowns {
        None => {}
        Some(breakdowns) => {
            let mut g = HashMap::new();
            for b in breakdowns {
                g.insert(b.to_owned(), ());
            }
            if g.len() != breakdowns.len() {
                return Err(PlatformError::BadRequest(
                    "use only unique breakdowns".to_string(),
                ));
            }
            for (idx, breakdown) in breakdowns.iter().enumerate() {
                match breakdown {
                    Breakdown::Property { property } => {
                        validate_event_property(
                            md,
                            project_id,
                            property,
                            format!("breakdown {idx}"),
                        )?;
                    }
                }
            }
        }
    }

    if req.segments.is_some() {
        return Err(PlatformError::Unimplemented(
            "segments are unimplemented yet".to_string(),
        ));
    }

    if let Some(hc) = &req.holding_constants {
        for (idx, prop) in hc.iter().enumerate() {
            validate_event_property(md, project_id, prop, format!("holding constant {idx}"))?;
        }
    }

    if let Some(filter) = &req.filter {
        match filter {
            Filter::DropOffOnAnyStep => {}
            Filter::DropOffOnStep { .. } => {}
            Filter::TimeToConvert { from, to } => {
                if from > to {
                    return Err(PlatformError::BadRequest(
                        "from time must be less than to time".to_string(),
                    ));
                }
            }
        }
    }

    match &req.filters {
        None => {}
        Some(filters) => {
            for filter_group in &filters.groups {
                if filters.groups.is_empty() {
                    return Err(PlatformError::BadRequest(
                        "filter_group field can't be empty".to_string(),
                    ));
                }
                for (filter_id, filter) in filter_group.filters.iter().enumerate() {
                    validate_event_filter(md, project_id, filter, filter_id, "".to_string())?;
                }
            }
        }
    }

    Ok(())
}

pub(crate) fn fix_request(
    req: common::funnel::Funnel,
) -> crate::Result<common::funnel::Funnel> {
    let mut out = req.clone();

    for (step_id, step) in req.steps.iter().enumerate() {
        for (event_id, event) in step.events.iter().enumerate() {
            if let Some(filters) = &event.filters {
                if filters.is_empty() {
                    out.steps[step_id].events[event_id].filters = None;
                }
            }
        }
    }

    if let Some(exclude) = &req.exclude
        && exclude.is_empty()
    {
        out.exclude = None;
    }

    if let Some(breakdowns) = &req.breakdowns
        && breakdowns.is_empty()
    {
        out.breakdowns = None;
    }

    if let Some(holding_constants) = &req.holding_constants
        && holding_constants.is_empty()
    {
        out.holding_constants = None;
    }

    if let Some(filters) = &req.filters {
        if filters.is_empty() {
            out.filters = None;
        }
    }
    Ok(out)
}