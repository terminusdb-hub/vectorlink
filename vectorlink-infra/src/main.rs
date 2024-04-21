use std::error::Error;

use aws_sdk_ec2::types::Filter;

fn filter<S1: Into<String>, T: Into<String>, S2: IntoIterator<Item = T>>(
    key: S1,
    val: S2,
) -> Filter {
    Filter::builder()
        .set_name(Some(key.into()))
        .set_values(Some(val.into_iter().map(|v| v.into()).collect()))
        .build()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_ec2::Client::new(&config);
    let regions = client
        .describe_regions()
        .set_filters(Some(vec![filter("region-name", ["us-east-1"])]))
        .send()
        .await?;
    eprintln!("{:?}", regions);
    let zones: Vec<String> = client
        .describe_availability_zones()
        .set_filters(Some(vec![filter("group-name", ["us-east-1"])]))
        .send()
        .await?
        .availability_zones
        .unwrap()
        .into_iter()
        .map(|z| z.zone_name.unwrap())
        .collect();

    eprintln!("{:?}", zones);

    let result = client.describe_instances().send().await?;
    let instances: Vec<_> = result
        .reservations
        .unwrap()
        .into_iter()
        .flat_map(|r| r.instances.unwrap())
        .collect();

    for instance in instances {
        let arch = instance.architecture().unwrap();
        let id = instance.instance_id().unwrap();
        let state = instance.state().unwrap().name().unwrap();
        //let lifecycle = instance.instance_lifecycle();
        let itype = instance.instance_type().unwrap();

        eprintln!("{id:?} ({itype:?} {arch:?}): {state:?}");
    }

    let tags = client
        .describe_tags()
        .set_filters(Some(vec![
            filter("key", ["Name"]),
            filter("resource-type", ["instance"]),
        ]))
        .send()
        .await?;

    for tag in tags.tags.unwrap() {
        eprintln!("{:?}: {:?}", tag.value(), tag.resource_id());
    }

    Ok(())
}
