use std::str::FromStr;

use chrono::DateTime;
use chrono_tz::Tz;
use rkyv::{
	boxed::{ArchivedBox, BoxResolver},
	out_field,
	ser::Serializer,
	with::{ArchiveWith, DeserializeWith, RefAsBox, SerializeWith},
	Archive,
	Fallible
};

pub struct DateTimeRkyv;

pub struct ArchivedDateTime {
	pub timestamp: i64,
	pub tz: ArchivedBox<str>,
}

pub struct DateTimeResolver {
	pub tz: BoxResolver<()>,
}

impl ArchiveWith<DateTime<Tz>> for DateTimeRkyv {
	type Archived = ArchivedDateTime;
	type Resolver = DateTimeResolver;

	unsafe fn resolve_with(
		datetime: &DateTime<Tz>,
		pos: usize,
		resolver: Self::Resolver,
		out: *mut Self::Archived,
	) {
		let timestamp = datetime.timestamp_nanos_opt().unwrap();
		let (fp, fo) = out_field!(out.timestamp);
		Archive::resolve(&timestamp, pos + fp, (), fo);
		let tz = datetime.timezone().name();
		let (fp, fo) = out_field!(out.tz);
		RefAsBox::resolve_with(&tz, pos + fp, resolver.tz, fo);
	}
}

impl<S> SerializeWith<DateTime<Tz>, S> for DateTimeRkyv
where
	S: Fallible + Serializer + ?Sized,
{
	fn serialize_with(datetime: &DateTime<Tz>, s: &mut S) -> Result<Self::Resolver, S::Error> {
		Ok(DateTimeResolver {
			tz: RefAsBox::serialize_with(&datetime.timezone().name(), s)?,
		})
	}
}

impl<D> DeserializeWith<ArchivedDateTime, DateTime<Tz>, D> for DateTimeRkyv
where
	D: Fallible + ?Sized,
	D::Error: From<<Tz as FromStr>::Err>,
{
	fn deserialize_with(archived: &ArchivedDateTime, _: &mut D) -> Result<DateTime<Tz>, D::Error> {
		let tz = archived.tz.as_ref().parse()?;

		Ok(DateTime::from_timestamp_nanos(archived.timestamp).with_timezone(&tz))
	}
}