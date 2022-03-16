Name:     redborder-cep
Version:  %{__version}
Release:  %{__release}%{?dist}

License:  GNU AGPLv3
URL:  https://github.com/redBorder/cep.git
Source0: %{name}-%{version}.tar.gz

BuildRequires: maven java-devel

Summary: redborder Complex Event Processor
Group: Services/Monitoring/Correlation
Requires: java

%description
%{summary}

%prep
%setup -qn %{name}-%{version}

%build
mvn clean package

%install
mkdir -p %{buildroot}/usr/lib/%{name}
install -D -m 644 target/cep-*-selfcontained.jar %{buildroot}/usr/lib/%{name}
mv %{buildroot}/usr/lib/%{name}/cep-*-selfcontained.jar %{buildroot}/usr/lib/%{name}/cep.jar
install -D -m 644 resources/systemd/redborder-cep.service %{buildroot}/usr/lib/systemd/system/redborder-cep.service

%clean
rm -rf %{buildroot}

%pre
getent group %{name} >/dev/null || groupadd -r %{name}
getent passwd %{name} >/dev/null || \
    useradd -r -g %{name} -d / -s /sbin/nologin \
    -c "User of %{name} service" %{name}
exit 0

%post
/sbin/ldconfig
systemctl daemon-reload

%postun -p /sbin/ldconfig

%files
%defattr(644,root,root)
/usr/lib/%{name}
/usr/lib/systemd/system/redborder-cep.service

%changelog
* Tue Feb 8 2022 Javier Rodriguez  <javiercrg@redborder.com> - 1.0.1-1
* Fri Jun 17 2016 Carlos J. Mateos  <cjmateos@redborder.com> - 1.0.0-1
- first spec version
