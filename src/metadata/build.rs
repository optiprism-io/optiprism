fn main() {
    prost_build::compile_protos(
        &[
            "src/pb/account.proto",
            "src/pb/bookmark.proto",
            "src/pb/custom_event.proto",
            "src/pb/dashboard.proto",
            "src/pb/event.proto",
            "src/pb/group.proto",
            "src/pb/organization.proto",
            "src/pb/project.proto",
            "src/pb/property.proto",
            "src/pb/report.proto",
            "src/pb/session.proto",
            "src/pb/team.proto",
            "src/pb/backup.proto",
            "src/pb/settings.proto",
        ],
        &["src"],
    )
    .unwrap();
}
